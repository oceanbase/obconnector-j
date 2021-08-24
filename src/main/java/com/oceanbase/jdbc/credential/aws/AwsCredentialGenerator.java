/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
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
package com.oceanbase.jdbc.credential.aws;

import java.util.Properties;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.credential.Credential;

public class AwsCredentialGenerator {

    private RdsIamAuthTokenGenerator generator;
    private GetIamAuthTokenRequest   request;
    private String                   userName;

    /**
     * AWS Generator.
     *
     * @param nonMappedOptions non standard options
     * @param userName user
     * @param hostAddress current server information
     */
    public AwsCredentialGenerator(Properties nonMappedOptions, String userName,
                                  HostAddress hostAddress) {
        // Build RDS IAM-auth token generator
        this.userName = userName;
        AWSCredentialsProvider awsCredentialsProvider;
        String accessKeyId = nonMappedOptions.getProperty("accessKeyId");
        String secretKey = nonMappedOptions.getProperty("secretKey");
        String region = nonMappedOptions.getProperty("region");

        if (accessKeyId != null && secretKey != null) {
            awsCredentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                accessKeyId, secretKey));
        } else {
            awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        }

        this.generator = RdsIamAuthTokenGenerator.builder().credentials(awsCredentialsProvider)
            .region(region != null ? region : new DefaultAwsRegionProviderChain().getRegion())
            .build();
        this.request = GetIamAuthTokenRequest.builder().hostname(hostAddress.host)
            .port(hostAddress.port).userName(userName).build();
    }

    public Credential getToken() {
        return new Credential(userName, generator.getAuthToken(this.request));
    }
}
