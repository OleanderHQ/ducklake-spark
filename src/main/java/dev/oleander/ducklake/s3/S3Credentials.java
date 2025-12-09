package dev.oleander.ducklake.s3;

import java.io.Serializable;

import lombok.Getter;

@Getter
public class S3Credentials implements Credentials, Serializable {
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;
  private final String region;

  public S3Credentials(String accessKeyId, String secretAccessKey, String sessionToken,
      String region) {
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.region = region;
  }
}
