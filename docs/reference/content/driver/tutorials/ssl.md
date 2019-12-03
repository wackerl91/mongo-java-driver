+++
date = "2016-05-29T22:05:03-04:00"
title = "TLS/SSL"
[menu.main]
  parent = "Connect to MongoDB"
  identifier = "SSL"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## TLS/SSL

The Java driver supports TLS/SSL connections to MongoDB servers using
the underlying support for TLS/SSL provided by the JDK. 
You can configure the driver to use TLS/SSL either with [`ConnectionString`]({{<apiref "com/mongodb/ConnectionString">}}) or with
[`MongoClientSettings`]({{<apiref "com/mongodb/MongoClientSettings">}}).
With the legacy MongoClient API you can use either [`MongoClientURI`]({{<apiref "com/mongodb/MongoClientURI">}}) or 
[`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}).

## MongoClient API (since 3.7)

### Specify TLS/SSL via `ConnectionString`

```java
com.mongodb.client.MongoClients;
com.mongodb.client.MongoClient;
```

To specify TLS/SSL with [`ConnectionString`]({{<apiref "com/mongodb/ConnectionString">}}), specify `ssl=true` as part of the connection
string, as in:

```java
MongoClient mongoClient = MongoClients.create("mongodb://localhost/?ssl=true");
```

### Specify TLS/SSL via `MongoClientSettings`

```java
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
```

To specify TLS/SSL with with [`MongoClientSettings`]({{<apiref "com/mongodb/MongoClientSettings">}}), set the `enabled` property to 
`true`, as in:

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .applyToSslSettings(builder -> 
            builder.enabled(true))
        .build();
MongoClient client = MongoClients.create(settings);
```

### Specify `SSLContext` via `MongoClientSettings`

```java
import javax.net.ssl.SSLContext;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
```

To specify the [`javax.net.ssl.SSLContext`](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) with 
[`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}), set the `sslContext` property, as in:

```java
SSLContext sslContext = ...
MongoClientSettings settings = MongoClientSettings.builder()
        .applyToSslSettings(builder -> {
                    builder.enabled(true);
                    builder.context(sslContext);
                })
        .build();
MongoClient client = MongoClients.create(settings);
```

## Legacy MongoClient API

### Specify TLS/SSL via `MongoClientURI`

```java
import com.mongodb.MongoClientURI;
import com.mongodb.MongoClient;
```

To specify TLS/SSL with [`MongoClientURI`]({{<apiref "com/mongodb/MongoClientURI">}}), specify `ssl=true` as part of the connection
string, as in:

```java
MongoClientURI uri = new MongoClientURI("mongodb://localhost/?ssl=true");
MongoClient mongoClient = new MongoClient(uri);
```

### Specify TLS/SSL via `MongoClientOptions`

```java
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClient;
```

To specify TLS/SSL with with [`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}), set the `sslEnabled` property to `true`, as in:

```java
MongoClientOptions options = MongoClientOptions.builder()
        .sslEnabled(true)
        .build();
MongoClient client = new MongoClient("localhost", options);
```

### Specify `SSLContext` via `MongoClientOptions`

```java
import javax.net.ssl.SSLContext;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClient;
```

To specify the [`javax.net.ssl.SSLContext`](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) with 
[`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}), set the `sslContext` property, as in:

```java
SSLContext sslContext = ...
MongoClientOptions options = MongoClientOptions.builder()
        .sslEnabled(true)
        .sslContext(sslContext)
        .build();
 MongoClient client = new MongoClient("localhost", options);
```

## Disable Hostname Verification

By default, the driver ensures that the hostname included in the
server's SSL certificate(s) matches the hostname(s) provided when
constructing a [`MongoClient()`]({{< apiref "com/mongodb/MongoClient.html">}}).

If your application needs to disable hostname verification, you must explicitly indicate
this in `MongoClientSettings`]({{<apiref "com/mongodb/MongoClientSettings">}}) 

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .applyToSslSettings(builder -> {
                    builder.enabled(true);
                    builder.invalidHostNameAllowed(true);
                })
        .build();
```

or, with the legacy `MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}), using the `sslInvalidHostNameAllowed` property:

```java
MongoClientOptions.builder()
        .sslEnabled(true)
        .sslInvalidHostNameAllowed(true)
        .build();
```

## JVM System Properties for TLS/SSL

A typical application will need to set several JVM system properties to
ensure that the client is able to validate the TLS/SSL certificate
presented by the server:

-  `javax.net.ssl.trustStore`:
      The path to a trust store containing the certificate of the
      signing authority

-  `javax.net.ssl.trustStorePassword`:
      The password to access this trust store

The trust store is typically created with the
[`keytool`](http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html)
command line program provided as part of the JDK. For example:

```bash
keytool -importcert -trustcacerts -file <path to certificate authority file>
            -keystore <path to trust store> -storepass <password>
```
A typical application will also need to set several JVM system
properties to ensure that the client presents an TLS/SSL certificate to the
MongoDB server:

- `javax.net.ssl.keyStore`
      The path to a key store containing the client's TLS/SSL certificates

- `javax.net.ssl.keyStorePassword`
      The password to access this key store

The key store is typically created with the
[`keytool`](http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html)
or the [`openssl`](https://www.openssl.org/docs/apps/openssl.html)
command line program.

For more information on configuring a Java application for TLS/SSL, please
refer to the [`JSSE Reference Guide`](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSS
ERefGuide.html).


### Forcing TLS 1.2

Some applications may want to force only the TLS 1.2 protocol. To do this, set the `jdk.tls.client.protocols` system property to "TLSv1.2".

Java runtime environments prior to Java 8 started to enable the TLS 1.2 protocol only in later updates, as shown in the previous section. For the driver to force the use of the TLS 1.2 protocol with a Java runtime environment prior to Java 8, ensure that the update has TLS 1.2 enabled.
