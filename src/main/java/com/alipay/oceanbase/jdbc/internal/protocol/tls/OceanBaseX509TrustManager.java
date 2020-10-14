package com.alipay.oceanbase.jdbc.internal.protocol.tls;

import java.io.*;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.alipay.oceanbase.jdbc.internal.util.SqlStates;
import com.alipay.oceanbase.jdbc.util.Options;

public class OceanBaseX509TrustManager implements X509TrustManager {

    private X509TrustManager trustManager;

    /**
     * MyX509TrustManager.
     *
     * @param options parsed url options
     * @throws SQLException exception
     */
    public OceanBaseX509TrustManager(Options options) throws SQLException {
        // if trustServerCertificate is true, will have a X509TrustManager implementation that validate
        // all.
        if (options.trustServerCertificate) {
            return;
        }

        KeyStore ks;
        try {
            ks = KeyStore.getInstance(options.trustStoreType != null ? options.trustStoreType
                : KeyStore.getDefaultType());
        } catch (GeneralSecurityException generalSecurityEx) {
            throw new SQLException("Failed to create keystore instance",
                SqlStates.CONNECTION_EXCEPTION.getSqlState(), generalSecurityEx);
        }

        InputStream inStream = null;
        try {
            if (options.trustStore != null) {
                // use the provided keyStore
                try {
                    String trustStore = options.trustStore;

                    try {
                        inStream = new URL(trustStore).openStream();
                    } catch (IOException ioexception) {
                        inStream = new FileInputStream(trustStore);
                    }
                    ks.load(inStream, options.trustStorePassword == null ? null
                        : options.trustStorePassword.toCharArray());

                } catch (GeneralSecurityException generalSecurityEx) {
                    throw new SQLException("Failed to create trustStore instance",
                        SqlStates.CONNECTION_EXCEPTION.getSqlState(), generalSecurityEx);
                } catch (FileNotFoundException fileNotFoundEx) {
                    throw new SQLException("Failed to find trustStore file. trustStore="
                                           + options.trustStore,
                        SqlStates.CONNECTION_EXCEPTION.getSqlState(), fileNotFoundEx);
                } catch (IOException ioEx) {
                    throw new SQLException("Failed to read trustStore file. trustStore="
                                           + options.trustStore,
                        SqlStates.CONNECTION_EXCEPTION.getSqlState(), ioEx);
                }
            } else {
                // generate a keyStore from the provided cert
                if (options.serverSslCert.startsWith("-----BEGIN CERTIFICATE-----")) {
                    inStream = new ByteArrayInputStream(options.serverSslCert.getBytes());
                } else if (options.serverSslCert.startsWith("classpath:")) {
                    // Load it from a classpath relative file
                    String classpathFile = options.serverSslCert.substring("classpath:".length());
                    inStream = Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream(classpathFile);
                } else {
                    try {
                        inStream = new FileInputStream(options.serverSslCert);
                    } catch (FileNotFoundException fileNotFoundEx) {
                        throw new SQLException("Failed to find serverSslCert file. serverSslCert="
                                               + options.serverSslCert,
                            SqlStates.CONNECTION_EXCEPTION.getSqlState(), fileNotFoundEx);
                    }
                }

                try {
                    // Note: KeyStore requires it be loaded even if you don't load anything into it
                    // (will be initialized with "javax.net.ssl.trustStore") values.
                    ks.load(null);
                    CertificateFactory cf = CertificateFactory.getInstance("X.509");
                    Collection<? extends Certificate> caList = cf.generateCertificates(inStream);
                    for (Certificate ca : caList) {
                        ks.setCertificateEntry(UUID.randomUUID().toString(), ca);
                    }
                } catch (IOException ioEx) {
                    throw new SQLException("Failed load keyStore",
                        SqlStates.CONNECTION_EXCEPTION.getSqlState(), ioEx);
                } catch (GeneralSecurityException generalSecurityEx) {
                    throw new SQLException(
                        "Failed to store certificate from serverSslCert into a keyStore",
                        SqlStates.CONNECTION_EXCEPTION.getSqlState(), generalSecurityEx);
                }
            }
        } finally {
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (IOException ioEx) {
                    // ignore error
                }
            }
        }
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
                .getDefaultAlgorithm());
            tmf.init(ks);
            for (TrustManager tm : tmf.getTrustManagers()) {
                if (tm instanceof X509TrustManager) {
                    trustManager = (X509TrustManager) tm;
                    break;
                }
            }
        } catch (NoSuchAlgorithmException noSuchAlgorithmEx) {
            throw new SQLException("Failed to create TrustManagerFactory default instance",
                SqlStates.CONNECTION_EXCEPTION.getSqlState(), noSuchAlgorithmEx);
        } catch (GeneralSecurityException generalSecurityEx) {
            throw new SQLException("Failed to initialize trust manager",
                SqlStates.CONNECTION_EXCEPTION.getSqlState(), generalSecurityEx);
        }

        if (trustManager == null) {
            throw new SQLException("No X509TrustManager found");
        }
    }

    /**
     * Check client trusted.
     *
     * @param x509Certificates certificate
     * @param string string
     * @throws CertificateException exception
     */
    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String string)
                                                                                     throws CertificateException {
        if (trustManager == null) {
            return;
        }
        trustManager.checkClientTrusted(x509Certificates, string);
    }

    /**
     * Check server trusted.
     *
     * @param x509Certificates certificate
     * @param string string
     * @throws CertificateException exception
     */
    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String string)
                                                                                     throws CertificateException {
        if (trustManager == null) {
            return;
        }
        trustManager.checkServerTrusted(x509Certificates, string);
    }

    public X509Certificate[] getAcceptedIssuers() {
        return null;
    }
}
