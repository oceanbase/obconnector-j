package com.alipay.oceanbase.jdbc.credential.aws;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.credential.Credential;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import java.util.Properties;

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
