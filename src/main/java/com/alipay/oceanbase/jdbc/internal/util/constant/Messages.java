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
package com.alipay.oceanbase.jdbc.internal.util.constant;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Messages {

    private static final String         BUNDLE_NAME = "com.alipay.oceanbase.jdbc.LocalizedErrorMessages";

    private static final ResourceBundle RESOURCE_BUNDLE;

    static {
        ResourceBundle temp = null;

        //
        // Overly-pedantic here, some appserver and JVM combos don't deal well with the no-args version, others don't deal well with the three-arg version, so
        // we need to try both :(
        //

        try {
            temp = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault(),
                Messages.class.getClassLoader());
        } catch (Throwable t) {
            try {
                temp = ResourceBundle.getBundle(BUNDLE_NAME);
            } catch (Throwable t2) {
                RuntimeException rt = new RuntimeException(
                    "Can't load resource bundle due to underlying exception " + t.toString());
                rt.initCause(t2);

                throw rt;
            }
        } finally {
            RESOURCE_BUNDLE = temp;
        }
    }

    /**
     * Returns the localized message for the given message key
     *
     * @param key
     *            the message key
     * @return The localized message for the key
     */
    public static String getString(String key) {
        if (RESOURCE_BUNDLE == null) {
            throw new RuntimeException("Localized messages from resource bundle '" + BUNDLE_NAME
                                       + "' not loaded during initialization of driver.");
        }

        try {
            if (key == null) {
                throw new IllegalArgumentException("Message key can not be null");
            }

            String message = RESOURCE_BUNDLE.getString(key);

            if (message == null) {
                message = "Missing error message for key '" + key + "'";
            }

            return message;
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }

    public static String getString(String key, Object[] args) {
        return MessageFormat.format(getString(key), args);
    }

    /**
     * Dis-allow construction ...
     */
    private Messages() {
    }
}
