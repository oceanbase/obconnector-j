/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase Technology Co.,Ltd.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.alipay.oceanbase.jdbc.credential.aws;

import static java.time.LocalDateTime.now;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.credential.Credential;
import com.alipay.oceanbase.jdbc.credential.CredentialPlugin;
import com.alipay.oceanbase.jdbc.util.Options;

/**
 * Permit AWS database IAM authentication.
 *
 * <p>Token is generated using IAM credential and region.
 *
 * <p>Implementation use SDK DefaultAWSCredentialsProviderChain and DefaultAwsRegionProviderChain
 * (environment variable / system properties, files, ...) or using connection string options :
 * accessKeyId, secretKey, region.
 *
 * @see <a
 *     href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html">DefaultAWSCredentialsProviderChain</a>
 * @see <a
 *     href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/regions/providers/DefaultAwsRegionProviderChain.html">DefaultAwsRegionProviderChain</a>
 */
public class AwsIamCredentialPlugin implements CredentialPlugin {

  private static final int TOKEN_TTL = 10;

  private static Map<KeyCache, IdentityExpire> cache = new ConcurrentHashMap<>();

  private AwsCredentialGenerator generator;
  private KeyCache key;

  @Override
  public String type() {
    return "AWS-IAM";
  }

  @Override
  public String name() {
    return "AWS IAM Authentication plugin";
  }

  @Override
  public boolean mustUseSsl() {
    return true;
  }

  @Override
  public CredentialPlugin initialize(Options options, String userName, HostAddress hostAddress)
      throws SQLException {
    try {
      Class.forName("com.amazonaws.auth.BasicAWSCredentials");
    } catch (ClassNotFoundException ex) {
      throw new SQLException(
          "Identity plugin 'AWS-IAM' is used without having AWS SDK in "
              + "classpath. "
              + "Please add 'com.amazonaws:aws-java-sdk-rds' to classpath");
    }
    this.generator = new AwsCredentialGenerator(options.nonMappedOptions, userName, hostAddress);
    this.key = new KeyCache(options, userName, hostAddress);
    return this;
  }

  @Override
  public Credential get() {
    IdentityExpire val = cache.get(key);
    if (val != null && val.isValid()) {
      return val.getCredential();
    }

    Credential credential = generator.getToken();
    cache.put(key, new IdentityExpire(credential));
    return credential;
  }

  private class IdentityExpire {

    private LocalDateTime expiration;
    private Credential credential;

    public IdentityExpire(Credential credential) {
      this.credential = credential;
      expiration = now().plusMinutes(TOKEN_TTL);
    }

    public boolean isValid() {
      return expiration.isAfter(now());
    }

    public Credential getCredential() {
      return credential;
    }
  }

  private class KeyCache {
    private Options options;
    private String userName;
    private HostAddress hostAddress;

    public KeyCache(Options options, String userName, HostAddress hostAddress) {
      this.options = options;
      this.userName = userName;
      this.hostAddress = hostAddress;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyCache keyCache = (KeyCache) o;
      return options.equals(keyCache.options)
          && Objects.equals(userName, keyCache.userName)
          && hostAddress.equals(keyCache.hostAddress);
    }

    @Override
    public int hashCode() {
      return Objects.hash(options, userName, hostAddress);
    }
  }
}
