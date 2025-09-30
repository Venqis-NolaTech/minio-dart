import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:dio/dio.dart';
import 'package:minio/minio.dart';
import 'package:minio/src/minio_helpers.dart';
import 'package:minio/src/minio_models.dart';
import 'package:minio/src/minio_s3.dart';
import 'package:minio/src/minio_sign.dart';
import 'package:minio/src/utils.dart';

class MinioRequest {
  MinioRequest(
    this.method,
    this.url, {
    this.onProgress,
    this.onSendProgress,
    this.cancelToken,
  });

  final String method;
  final Uri url;
  final void Function(int)? onProgress;
  final void Function(MinioRequestProgressData)? onSendProgress;
  final CancelToken? cancelToken;

  dynamic body;
  Map<String, String> headers = {};

  MinioRequest replace({
    String? method,
    Uri? url,
    Map<String, String>? headers,
    body,
  }) {
    final result = MinioRequest(method ?? this.method, url ?? this.url);
    result.body = body ?? this.body;
    result.headers.addAll(headers ?? this.headers);
    return result;
  }

  Future<Response<ResponseBody>> send() async {
    var dio = Dio();
    dio.options.headers = headers;
    dio.options.responseType = ResponseType.stream;

    try {
      Response<ResponseBody> response = await dio.request(
        url.toString(),
        data: body,
        cancelToken: cancelToken,
        options: Options(
          method: method,
          headers: headers,
          responseType: ResponseType.stream,
        ),
        onReceiveProgress: (p, t) => onProgress?.call(p),
        onSendProgress: (p, t) {
          final sentFileSize = p;
          final totalFileSize = t;

          final progress = (p / t);

          onProgress?.call(p);
          onSendProgress?.call(
            MinioRequestProgressData(
              totalFileSize: totalFileSize,
              sentFileSize: sentFileSize,
              progress: progress,
            ),
          );
        },
      );

      return response;
    } on DioException catch (e) {
      return Response(
        requestOptions: e.requestOptions,
        extra: e.response?.extra,
        data: e.response?.data,
        headers: e.response?.headers,
        statusCode: e.response?.statusCode,
        statusMessage: e.response?.statusMessage,
      );
    }
  }
}

/// An HTTP response where the entire response body is known in advance.
class MinioResponse {
  /// Create a new HTTP response with a byte array body.
  MinioResponse(
    this.bodyBytes,
    this.statusCode, {
    this.request,
    this.headers = const {},
    this.isRedirect = false,
    this.persistentConnection = true,
    this.reasonPhrase,
  });

  /// Response with a byte array body.
  Stream<Uint8List> get stream =>
      Stream.fromIterable([Uint8List.fromList(bodyBytes)]);

  /// Status code of the response.
  final int statusCode;

  /// The request for which this response was received.
  final RequestOptions? request;

  /// The headers associated with this response.
  final Map<String, String> headers;

  /// Whether this response is a redirect.
  final bool isRedirect;

  /// Whether the connection to the server should be kept alive after this response is received.
  final bool persistentConnection;

  /// The reason phrase associated with the status code.
  final String? reasonPhrase;

  final List<int> bodyBytes;

  /// Decode bodyBytes to a UTF-8 string when accessing as body
  String get body => utf8.decode(bodyBytes);

  /// Content stream of the response.

  /// Content length of the response.
  int get contentLength => int.parse(headers['content-length'] ?? '-1');

  static Future<MinioResponse> fromStream(
    Response<ResponseBody> response,
  ) async {
    // Read and accumulate all bytes from the response stream
    final bodyBytes = <int>[];
    await for (var chunk in response.data!.stream) {
      bodyBytes.addAll(chunk);
    }

    return MinioResponse(
      bodyBytes,
      response.statusCode ?? 0,
      request: response.requestOptions,
      headers: response.headers.map.map(
        (key, value) => MapEntry(
          key,
          value.join(','),
        ),
      ),
      isRedirect: response.isRedirect,
      persistentConnection: response.requestOptions.persistentConnection,
      reasonPhrase: response.statusMessage,
    );
  }
}

class MinioClient {
  MinioClient(this.minio) {
    anonymous = minio.accessKey.isEmpty && minio.secretKey.isEmpty;
    enableSHA256 = !anonymous && !minio.useSSL;
    port = minio.port;
  }

  final Minio minio;
  final String userAgent = 'MinIO (Unknown; Unknown) minio-dart/2.0.0';

  late bool enableSHA256;
  late bool anonymous;
  late final int port;

  Future<Response<ResponseBody>> _request({
    required String method,
    String? bucket,
    String? object,
    String? region,
    String? resource,
    dynamic payload = '',
    Map<String, dynamic>? queries,
    Map<String, String>? headers,
    void Function(int)? onProgress,
    void Function(MinioRequestProgressData)? onSendProgress,
    CancelToken? cancelToken,
  }) async {
    if (bucket != null) {
      region ??= await minio.getBucketRegion(bucket);
    }

    region ??= 'us-east-1';

    final request = getBaseRequest(
      method,
      bucket,
      object,
      region,
      resource,
      queries,
      headers,
      onProgress,
      onSendProgress,
      cancelToken,
    );
    request.body = payload;

    final date = DateTime.now().toUtc();
    final sha256sum = enableSHA256 ? sha256Hex(payload) : 'UNSIGNED-PAYLOAD';
    request.headers.addAll({
      'user-agent': userAgent,
      'x-amz-date': makeDateLong(date),
      'x-amz-content-sha256': sha256sum,
    });

    if (minio.sessionToken != null) {
      request.headers['x-amz-security-token'] = minio.sessionToken!;
    }

    final authorization = signV4(minio, request, date, region);
    request.headers['authorization'] = authorization;
    logRequest(request);
    final response = await request.send();
    return response;
  }

  Future<MinioResponse> request({
    required String method,
    String? bucket,
    String? object,
    String? region,
    String? resource,
    dynamic payload = '',
    Map<String, dynamic>? queries,
    Map<String, String>? headers,
    void Function(int)? onProgress,
    void Function(MinioRequestProgressData)? onSendProgress,
    CancelToken? cancelToken,
  }) async {
    final stream = await _request(
      method: method,
      bucket: bucket,
      object: object,
      region: region,
      payload: payload,
      resource: resource,
      queries: queries,
      headers: headers,
      onProgress: onProgress,
      onSendProgress: onSendProgress,
      cancelToken: cancelToken,
    );

    final response = await MinioResponse.fromStream(stream);
    logResponse(response);

    return response;
  }

  Future<MinioResponse> requestStream({
    required String method,
    String? bucket,
    String? object,
    String? region,
    String? resource,
    dynamic payload = '',
    Map<String, dynamic>? queries,
    Map<String, String>? headers,
    CancelToken? cancelToken,
  }) async {
    final response_ = await _request(
      method: method,
      bucket: bucket,
      object: object,
      region: region,
      payload: payload,
      resource: resource,
      queries: queries,
      headers: headers,
      cancelToken: cancelToken,
    );

    MinioResponse response = await MinioResponse.fromStream(response_);

    logResponse(response);
    return response;
  }

  MinioRequest getBaseRequest(
    String method,
    String? bucket,
    String? object,
    String region,
    String? resource,
    Map<String, dynamic>? queries,
    Map<String, String>? headers,
    void Function(int)? onProgress,
    void Function(MinioRequestProgressData)? onSendProgress,
    CancelToken? cancelToken,
  ) {
    final url = getRequestUrl(bucket, object, resource, queries);
    final request = MinioRequest(
      method,
      url,
      onProgress: onProgress,
      onSendProgress: onSendProgress,
      cancelToken: cancelToken,
    );
    request.headers['host'] = url.authority;

    if (headers != null) {
      request.headers.addAll(headers);
    }

    return request;
  }

  Uri getRequestUrl(
    String? bucket,
    String? object,
    String? resource,
    Map<String, dynamic>? queries,
  ) {
    var host = minio.endPoint.toLowerCase();
    var path = '/';

    bool pathStyle = minio.pathStyle ?? true;
    if (isAmazonEndpoint(host)) {
      host = getS3Endpoint(minio.region!);
      pathStyle = !isVirtualHostStyle(host, minio.useSSL, bucket);
    }

    if (!pathStyle) {
      if (bucket != null) host = '$bucket.$host';
      if (object != null) path = '/$object';
    } else {
      if (bucket != null) path = '/$bucket';
      if (object != null) path = '/$bucket/$object';
    }

    final query = StringBuffer();
    if (resource != null) {
      query.write(resource);
    }
    if (queries != null) {
      if (query.isNotEmpty) query.write('&');
      query.write(encodeQueries(queries));
    }

    return Uri(
      scheme: minio.useSSL ? 'https' : 'http',
      host: host,
      port: minio.port,
      pathSegments: path.split('/'),
      query: query.toString(),
    );
  }

  void logRequest(MinioRequest request) {
    if (!minio.enableTrace) return;

    final buffer = StringBuffer();
    buffer.writeln('REQUEST: ${request.method} ${request.url}');
    for (var header in request.headers.entries) {
      buffer.writeln('${header.key}: ${header.value}');
    }

    if (request.body is List<int>) {
      buffer.writeln('List<int> of size ${request.body.length}');
    } else {
      buffer.writeln(request.body);
    }

    print(buffer.toString());
  }

  void logResponse(MinioResponse response) {
    if (!minio.enableTrace) return;

    final buffer = StringBuffer();
    buffer.writeln('RESPONSE: ${response.statusCode} ${response.reasonPhrase}');
    for (var header in response.headers.entries) {
      buffer.writeln('${header.key}: ${header.value}');
    }

    if (response.request?.responseType == ResponseType.json) {
      buffer.writeln(response.body);
    } else if (response.request?.responseType == ResponseType.stream) {
      buffer.writeln('STREAMED BODY');
    }

    print(buffer.toString());
  }
}
