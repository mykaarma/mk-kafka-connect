package com.mykaarma.kafka.connect.smt;

import java.io.IOException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Writes bodies to S3 with static access keys. The credential only needs s3:PutObject. */
class S3BodyStore implements BodyStore {

  private final S3Client s3;
  private final String bucket;

  S3BodyStore(String bucket, String region, String accessKeyId, String secretAccessKey) {
    this.bucket = bucket;
    this.s3 = S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .build();
  }

  @Override
  public void put(String key, byte[] body) throws IOException {
    try {
      s3.putObject(
          PutObjectRequest.builder().bucket(bucket).key(key).contentType("application/json").build(),
          RequestBody.fromBytes(body));
    } catch (RuntimeException e) {
      throw new IOException("S3 PutObject failed for " + key, e);
    }
  }

  @Override
  public String bucket() {
    return bucket;
  }
}
