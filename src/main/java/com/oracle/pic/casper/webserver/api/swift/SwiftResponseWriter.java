package com.oracle.pic.casper.webserver.api.swift;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.oracle.pic.casper.common.rest.ByteRange;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.vertx.stream.AbortableReadStream;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftContainer;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftBulkDeleteResponse;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Writes responses out for the Swift API, both errors and happy-path cases.
 *
 * Swift supports plaintext, JSON, and XML response formats.  We only support JSON and XML for now, but if we ever add
 * support for plaintext, it will go here.
 *
 * Note:  if you're adding support for plain text, ensure that all browsers are safe from XSS scripting attacks.
 * See https://jira.oci.oraclecorp.com/browse/CASPER-448 for an example.  If an attacker can get a user to visit a
 * page that contains tenant names, bucket names, or object names that they control, then the attacker can get the
 * user to load an HTML {@code <script>} tag.  Browsers shouldn't execute script tags on documents with content
 * type "text/plain", but check and make sure.  If the browser executes the script, you've got a XSS vulnerability
 * and you'll need HTML escaping.
 */
public class SwiftResponseWriter {

    private final ObjectMapper jsonMapper;
    private final XmlMapper xmlMapper;
    private final XMLOutputFactory xmlOutputFactory;

    public SwiftResponseWriter(ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
        this.xmlMapper = new XmlMapper();
        this.xmlOutputFactory = XMLOutputFactory.newFactory();
    }

    public void writeContainersResponse(RoutingContext routingContext, SwiftResponseFormat format,
                                        List<SwiftContainer> content) {
        SwiftHttpHeaderHelpers.writeCommonHeaders(routingContext);

        switch (format) {
            case JSON:
                writeJsonResponse(routingContext, content);
                break;
            case XML:
                writeXmlContainersResponse(routingContext, content);
                break;
            default:
                throw new RuntimeException("Unsupported response format: " + format);
        }
   }

    public void writeObjectsResponse(RoutingContext routingContext, SwiftResponseFormat format,
                                     List<SwiftObject> content, SwiftContainer swiftContainer) {
        SwiftHttpHeaderHelpers.writeCommonHeaders(routingContext);
        SwiftHttpHeaderHelpers.addUserContainerMetadataToHttpHeaders(routingContext.response(),
                swiftContainer.getMetadata());

        switch (format) {
            case JSON:
                writeJsonResponse(routingContext, content);
                break;
            case XML:
                writeXmlObjectsResponse(routingContext, content);
                break;
            default:
                throw new RuntimeException("Unsupported response format: " + format);
        }
    }

    public void writeDeletesResponse(RoutingContext routingContext, SwiftResponseFormat format,
                                     SwiftBulkDeleteResponse content) {
        SwiftHttpHeaderHelpers.writeCommonHeaders(routingContext);

        switch (format) {
            case JSON:
                writeJsonResponse(routingContext, content);
                break;
            case XML:
                writeXmlResponse(routingContext, content);
                break;
            default:
                throw new RuntimeException("Unsupported response format: " + format);
        }
    }

    /**
     * Writes an HTTP response with a Swift "account" (list of containers) serialized as XML in the body.
     *
     * This writes out an HTTP response with the following headers and an account tag wrapping the raw list of
     * containers passed in:
     *  - Content-Type: application/xml; charset=utf-8
     *  - Content-Length: body length
     *
     * Other headers that need to go out on the response should be set on the response by the caller before calling
     * this method.
     *
     * @param routingContext the Vert.x routing context for this request
     * @param containers     the list of containers in the account
     */
    // TODO(jfriedly):  See if you can figure out a way to abstract out some of the duplicated code in this method.
    // May be hard with the scoping requirements.
    public void writeXmlContainersResponse(RoutingContext routingContext, List<SwiftContainer> containers) {
        final HttpServerRequest request = routingContext.request();
        final HttpServerResponse response = routingContext.response();

        XMLStreamWriter streamWriter = null;
        try (StringWriter body = new StringWriter()) {
            streamWriter = xmlOutputFactory.createXMLStreamWriter(body);
            streamWriter.writeStartDocument("UTF-8", "1.0");
            streamWriter.writeStartElement("account");
            streamWriter.writeAttribute("name", request.getParam(SwiftApi.ACCOUNT_PARAM));
            for (SwiftContainer container : containers) {
                xmlMapper.writeValue(streamWriter, container);
            }
            streamWriter.writeEndElement();
            streamWriter.writeEndDocument();

            // Vert.x will set the Content-Length header for us.
            response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_XML_UTF8)
                .setStatusCode(HttpResponseStatus.OK)
                .end(body.toString());
            streamWriter.close();
        } catch (XMLStreamException xmlse) {
            tryCloseXmlStreamWriter(streamWriter);
            throw new RuntimeException("Could not create an XMLStreamWriter or write basic tags", xmlse);
        } catch (IOException ioe) {
            tryCloseXmlStreamWriter(streamWriter);
            throw new RuntimeException("Could not write out custom XML tags", ioe);
        }
    }

    /**
     * Writes an HTTP response with a Swift "container" (list of objects) serialized as XML in the body.
     *
     * This writes out an HTTP response with the following headers and a container tag wrapping the raw list of objects
     * passed in:
     *  - Content-Type: application/xml; charset=utf-8
     *  - Content-Length: body length
     *
     * Other headers that need to go out on the response should be set on the response by the caller before calling
     * this method.
     *
     * @param routingContext the Vert.x routing context for this request
     * @param objects        the list of objects in the container
     */
    public void writeXmlObjectsResponse(RoutingContext routingContext, List<SwiftObject> objects) {
        final HttpServerRequest request = routingContext.request();
        final HttpServerResponse response = routingContext.response();

        XMLStreamWriter streamWriter = null;
        try (StringWriter body = new StringWriter()) {
            streamWriter = xmlOutputFactory.createXMLStreamWriter(body);
            streamWriter.writeStartDocument("UTF-8", "1.0");
            streamWriter.writeStartElement("container");
            streamWriter.writeAttribute("name", request.getParam(SwiftApi.CONTAINER_PARAM));
            for (SwiftObject object : objects) {
                xmlMapper.writeValue(streamWriter, object);
            }
            streamWriter.writeEndElement();
            streamWriter.writeEndDocument();

            // Vert.x will set the Content-Length header for us.
            response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_XML_UTF8)
                .setStatusCode(HttpResponseStatus.OK)
                .end(body.toString());
            streamWriter.close();
        } catch (XMLStreamException xmlse) {
            tryCloseXmlStreamWriter(streamWriter);
            throw new RuntimeException("Could not create an XMLStreamWriter or write basic tags", xmlse);
        } catch (IOException ioe) {
            tryCloseXmlStreamWriter(streamWriter);
            throw new RuntimeException("Could not write out custom XML tags", ioe);
        }

    }

    public <T> void writeXmlResponse(RoutingContext routingContext, T content) {
        final HttpServerResponse response = routingContext.response();

        xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true);
        final String body;
        try {
            body = xmlMapper.writeValueAsString(content);
        } catch (JsonProcessingException ioex) {
            throw new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR, "Internal server error",
                routingContext.request().path(), ioex);
        } finally {
            xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, false);
        }

        // Vert.x will set the Content-Length header for us.
        response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_XML_UTF8)
            .setStatusCode(HttpResponseStatus.OK)
            .end(body);
    }

    /**
     * Writes an HTTP response with a serialized JSON object in the body, ending the request.
     *
     * This writes out an HTTP response with the content object serialized as JSON in the body and adds these headers:
     *  - Content-Type: application/json; charset=utf-8
     *  - Content-Length: body length
     *
     * Other headers that need to go out on the response should be set on the response by the caller before calling
     * this method.
     *
     * @param routingContext the Vert.x routing context for this request
     * @param content        the object to serialize to the body of the response.
     * @param <T>            the class of the content object.
     */
    public <T> void writeJsonResponse(RoutingContext routingContext, T content) {
        final HttpServerResponse response = routingContext.response();

        final String body;
        try {
            body = jsonMapper.writeValueAsString(content);
        } catch (JsonProcessingException ioex) {
            throw new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR, "Internal server error",
                routingContext.request().path(), ioex);
        }

        // Vert.x will set the Content-Length header for us.
        response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON_UTF8)
                .setStatusCode(HttpResponseStatus.OK)
                .end(body);
    }

    /**
     * Writes out an HTTP 204 No Content response
     *
     * Callers are expected to have already added any headers that they want on the response.
     *
     * This method is useful for HEAD requests, and may also find use if we ever support the plaintext format for the
     * Swift list APIs.  (A Swift list containers or list objects call with Accept: text/plain [the default] will
     * return the names of the containers or objects in a newline-separated list.  If there are none, the response
     * body is empty and Swift actually returns 204 instead of 200.)
     */
    public void writeEmptyResponse(RoutingContext routingContext, String format) {
        final HttpServerResponse response = routingContext.response();

        SwiftHttpHeaderHelpers.writeCommonHeaders(routingContext);

        response.putHeader(HttpHeaders.CONTENT_TYPE, format)
                .putHeader(HttpHeaders.CONTENT_LENGTH, "0")
                .setStatusCode(HttpResponseStatus.NO_CONTENT)
                .end();
    }

    /**
     * Writes out an HTTP 201 Created response with no body
     *
     * Callers are expected to have already added any headers that they want on the response.
     *
     * This method is used by the PUT object/container handler to indicate that an object
     * was successfully created (or updated).
     */
    public void writeCreatedResponse(RoutingContext routingContext) {
        final HttpServerResponse response = routingContext.response();

        SwiftHttpHeaderHelpers.writeCommonHeaders(routingContext);

        response.setStatusCode(HttpResponseStatus.CREATED)
                .end();
    }

    /**
     * Writes out the response to HEAD object requests
     *
     * @param routingContext The routing context for the request.
     * @param objectMetadata The object metadata (handler will throw 404 if not found).
     * @param objectSizeInBytes The size of the entire object, even if it's a DLO or SLO
     * @param notModified Whether or not the response should be a 304 Not Modified response.
     */
    public void writeHeadObjectResponse(RoutingContext routingContext,
                                        ObjectMetadata objectMetadata,
                                        long objectSizeInBytes,
                                        boolean notModified) {
        final HttpServerResponse response = routingContext.response();

        SwiftHttpHeaderHelpers.writeCommonHeaders(routingContext);
        SwiftHttpHeaderHelpers.writeObjectHeaders(response, objectMetadata);

        if (notModified) {
            // HEAD If-None-Match ETag matches.  Return 304 Not Modified, with proper object headers.
            response.setStatusCode(HttpResponseStatus.NOT_MODIFIED)
                    .putHeader(HttpHeaders.CONTENT_LENGTH, "0")
                    .end();
        } else {
            response.setStatusCode(HttpResponseStatus.OK)
                    .putHeader(HttpHeaders.CONTENT_LENGTH, Long.toString(objectSizeInBytes))
                    .end();
        }
    }

    /**
     * Writes out the response to GET object requests
     *
     * @param routingContext The routing context for the request.
     * @param objectMetadata The object metadata (handler will throw 404 if not found).
     * @param byteRange The range of bytes that the caller asked for.
     * @param stream The object body data stream
     * @param objectSizeInBytes The size of the entire object, even if it's a DLO or SLO
     * @param notModified Whether or not the response should be a 304 Not Modified response.
     */
    public CompletableFuture<Void> writeGetObjectResponse(
            RoutingContext routingContext,
            ObjectMetadata objectMetadata,
            @Nullable ByteRange byteRange,
            AbortableReadStream<Buffer> stream,
            long objectSizeInBytes,
            boolean notModified) {
        final HttpServerResponse response = routingContext.response();

        SwiftHttpHeaderHelpers.writeCommonHeaders(routingContext);
        SwiftHttpHeaderHelpers.writeObjectHeaders(response, objectMetadata);

        if (notModified) {
            // If-None-Match ETag matches.  Return 304 Not Modified, with proper object headers
            response.setStatusCode(HttpResponseStatus.NOT_MODIFIED)
                    .putHeader(HttpHeaders.CONTENT_LENGTH, "0")
                    .end();
            return CompletableFuture.completedFuture(null);
        } else {
            // Range GET requests that did not result in 304.
            // Range requests will still return the object's ETag, even though it no longer represents the MD-5 of
            // the response body.  This is consistent with the Swift reference implementation.
            if (byteRange != null) {
                HttpContentHelpers.writeContentRangeHeaders(routingContext, byteRange, objectSizeInBytes);
            } else {
                response.setStatusCode(HttpResponseStatus.OK)
                    .putHeader(HttpHeaders.CONTENT_LENGTH, Long.toString(objectSizeInBytes));
            }
            return HttpContentHelpers.pumpResponseContent(routingContext, response, stream);
        }

    }

    private void tryCloseXmlStreamWriter(@Nullable XMLStreamWriter streamWriter) {
        if (streamWriter != null) {
            try {
                streamWriter.close();
            } catch (XMLStreamException xmlse) {
                throw new RuntimeException("Could not close XMLStreamWriter", xmlse);
            }
        }
    }
}
