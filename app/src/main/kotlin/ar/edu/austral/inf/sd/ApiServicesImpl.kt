package ar.edu.austral.inf.sd

import ar.edu.austral.inf.sd.server.api.BadRequestException
import ar.edu.austral.inf.sd.server.api.PlayApiService
import ar.edu.austral.inf.sd.server.api.RegisterNodeApiService
import ar.edu.austral.inf.sd.server.api.RelayApiService
import ar.edu.austral.inf.sd.server.model.PlayResponse
import ar.edu.austral.inf.sd.server.model.RegisterResponse
import ar.edu.austral.inf.sd.server.model.Signature
import ar.edu.austral.inf.sd.server.model.Signatures
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.flow.update
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.web.client.RestTemplate
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.random.Random

@Component
class ApiServicesImpl: RegisterNodeApiService, RelayApiService, PlayApiService {

    @Value("\${server.name:nada}")
    private val myServerName: String = ""
    @Value("\${server.port:8080}")
    private val myServerPort: Int = 0
    private val nodes: MutableList<RegisterResponse> = mutableListOf()
    private var nextNode: RegisterResponse? = null
    private val messageDigest = MessageDigest.getInstance("SHA-512")
    private val salt = newSalt()
    private val currentRequest
        get() = (RequestContextHolder.getRequestAttributes() as ServletRequestAttributes).request
    private var resultReady = CountDownLatch(1)
    private var currentMessageWaiting = MutableStateFlow<PlayResponse?>(null)
    private var currentMessageResponse = MutableStateFlow<PlayResponse?>(null)

    override fun registerNode(host: String?, port: Int?, name: String?): RegisterResponse {
        println("registerNode called with host: $host, port: $port, name: $name")
        val nextNode = if (nodes.isEmpty()) {
            println("No nodes registered, adding current server as the first node.")
            val selfNode = RegisterResponse(currentRequest.serverName, myServerPort, "", "")
            nodes.add(selfNode)
            selfNode
        } else {
            println("Returning the last registered node.")
            nodes.last()
        }
        val uuid = UUID.randomUUID().toString()
        val newNode = RegisterResponse(host!!, port!!, uuid, newSalt())
        nodes.add(newNode)
        println("New node registered: $newNode")
        return RegisterResponse(nextNode.nextHost, nextNode.nextPort, uuid, newSalt())
    }

    override fun relayMessage(message: String, signatures: Signatures): Signature {
        println("relayMessage called with message: $message, signatures: $signatures")
        val receivedHash = doHash(message.encodeToByteArray(), salt)
        val receivedContentType = currentRequest.getPart("message")?.contentType ?: "nada"
        val receivedLength = message.length
        println("Calculated hash: $receivedHash, content type: $receivedContentType, length: $receivedLength")

        if (nextNode != null) {
            println("Next node exists, relaying message.")
            sendRelayMessage(message, receivedContentType, nextNode!!, signatures)
        } else {
            println("No next node, processing the message locally.")
            if (currentMessageWaiting.value == null) throw BadRequestException("no waiting message")
            val current = currentMessageWaiting.getAndUpdate { null }!!
            val response = current.copy(
                contentResult = if (receivedHash == current.originalHash) "Success" else "Failure",
                receivedHash = receivedHash,
                receivedLength = receivedLength,
                receivedContentType = receivedContentType,
                signatures = signatures
            )
            currentMessageResponse.update { response }
            resultReady.countDown()
        }
        return Signature(
            name = myServerName,
            hash = receivedHash,
            contentType = receivedContentType,
            contentLength = receivedLength
        )
    }

    override fun sendMessage(body: String): PlayResponse {
        println("sendMessage called with body: $body")
        if (nodes.isEmpty()) {
            println("No nodes registered, adding current server as the first node.")
            val me = RegisterResponse(currentRequest.serverName, myServerPort, "", "")
            nodes.add(me)
        }
        currentMessageWaiting.update { newResponse(body) }
        val contentType = currentRequest.contentType
        println("Sending relay message with content type: $contentType")
        sendRelayMessage(body, contentType, nodes.last(), Signatures(listOf()))
        resultReady.await()
        resultReady = CountDownLatch(1)
        println("Result ready, returning response.")
        return currentMessageResponse.value!!
    }

    internal fun registerToServer(registerHost: String, registerPort: Int) {
        println("registerToServer called with registerHost: $registerHost, registerPort: $registerPort")
        val serverUrl = "http://${registerHost}:${registerPort}/register-node?host=localhost&port=${myServerPort}&name=${myServerName}"
        val restTemplate = RestTemplate()
        val nextNodeResponse = restTemplate.postForEntity(serverUrl, null,  RegisterResponse::class.java)
        val registerNodeResponse: RegisterResponse = nextNodeResponse.body!!
        println("nextNode = $registerNodeResponse")
        nextNode = with(registerNodeResponse) { RegisterResponse(nextHost, nextPort, uuid, hash) }
    }

    private fun sendRelayMessage(body: String, contentType: String, relayNode: RegisterResponse, signatures: Signatures) {
        println("sendRelayMessage called with body: $body, relayNode: $relayNode")
        val restTemplate = RestTemplate()
        val apiUrl = "http://${relayNode.nextHost}:${relayNode.nextPort}/relay"
        val mySignature = clientSign(body, contentType)
        val newSignatures = Signatures(signatures.items + mySignature)
        val multipartRequest: MultiValueMap<String, Any> = LinkedMultiValueMap()
        multipartRequest.add("message", body)
        multipartRequest.add("signatures", newSignatures)

        val headers = HttpHeaders()
        headers.contentType = MediaType.MULTIPART_FORM_DATA
        restTemplate.postForEntity(apiUrl, multipartRequest, Signature::class.java, headers)
        println("Message relayed to $apiUrl")
    }

    private fun clientSign(message: String, contentType: String): Signature {
        println("clientSign called with message: $message, contentType: $contentType")
        val receivedHash = doHash(message.encodeToByteArray(), salt)
        return Signature(myServerName, receivedHash, contentType, message.length)
    }

    private fun newResponse(body: String) = PlayResponse(
        "Unknown",
        currentRequest.contentType,
        body.length,
        doHash(body.encodeToByteArray(), salt),
        "Unknown",
        -1,
        "N/A",
        Signatures(listOf())
    )

    private fun doHash(body: ByteArray, salt: String):  String {
        println("doHash called with body size: ${body.size}, salt: $salt")
        val saltBytes = Base64.getDecoder().decode(salt)
        messageDigest.update(saltBytes)
        val digest = messageDigest.digest(body)
        val hash = Base64.getEncoder().encodeToString(digest)
        println("Generated hash: $hash")
        return hash
    }

    companion object {
        fun newSalt(): String {
            val salt = Base64.getEncoder().encodeToString(Random.nextBytes(9))
            println("Generated new salt: $salt")
            return salt
        }
    }
}
