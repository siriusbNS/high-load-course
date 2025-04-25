package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()

        private const val MAX_RETRY_ATTEMPTS = 1000
        private const val HEDGED_DELAY_MS = 2400L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec  = properties.rateLimitPerSec
    private val maxParallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val window = NonBlockingOngoingWindow(maxParallelRequests)
    private val semaphore = Semaphore(maxParallelRequests)

    private val httpClient: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(3))
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        fun attemptRequest(attempt: Int) {
            if (now() + averageProcessingTime.toMillis() >= deadline) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return
            }

            if (!semaphore.tryAcquire()) {
                return
            }

            if (!rateLimiter.tick()) {
                semaphore.release()
                return
            }

            if (window.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
                semaphore.release()
                return
            }

            val request = HttpRequest.newBuilder().uri(URI.create(
                        "http://localhost:1234/external/process" +
                                "?serviceName=$serviceName&accountName=$accountName" +
                                "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .version(HttpClient.Version.HTTP_2)
                .POST(HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofMillis(deadline - now()))
                .build()

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .orTimeout(deadline - now(), TimeUnit.MILLISECONDS)
                .thenAcceptAsync { response ->
                    try {
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] Failed to parse response " +
                                        "for txId=$transactionId, payment=$paymentId, code=${response.statusCode()}"
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.info(
                            "[$accountName] Response for txId=$transactionId, payment=$paymentId: " +
                                    "result=${body.result}, message=${body.message}"
                        )

                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    } catch (e: Exception) {
                        logger.error("[$accountName] Unexpected error processing response", e)
                    }
                }
                .exceptionally { ex ->
                    if (attempt < MAX_RETRY_ATTEMPTS) {
                        logger.warn("[$accountName] Retrying paymentId=$paymentId, attempt=$attempt due to ${ex.message}")
                        attemptRequest(attempt + 1)
                    } else {
                        logger.error("[$accountName] Payment failed for paymentId=$paymentId after $attempt attempts", ex)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = ex.message)
                        }
                    }
                    null
                }
                .whenComplete { _, _ ->
                    semaphore.release()
                    window.releaseWindow()
                }
        }

        attemptRequest(1)
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now(): Long = System.currentTimeMillis()
