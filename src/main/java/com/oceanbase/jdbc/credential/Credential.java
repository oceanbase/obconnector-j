/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */
package com.oceanbase.jdbc.credential;

public class Credential {
    private String user;
    private String password;
    private String proxyUser;

    public Credential(String user, String password) {
        this.user = user;
        this.password = password;
    }

    public Credential(String user, String password, boolean useProxyUser) {
        this.user = user;
        this.password = password;
        if (useProxyUser) {
            setUsernameAndProxyUsername();
        }
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    private void setUsernameAndProxyUsername() {
        String originUsername = this.user;
        String username = this.user;
        try {
            username = username.trim();
            int len = username.length();
            int[] user_pos = {0, len};
            int[] proxy_user_pos = new int[2];
            int left_info_pos = len;
            char lastChar = 0;
            boolean hasQuota = false;

            char[] usernameCharArray = username.toCharArray();
            for (int i = 0; i < usernameCharArray.length; i++) {
                char currentChar = usernameCharArray[i];

                if (i == 0 && (currentChar == '\'' || currentChar == '\"')) {
                    hasQuota = true;
                }

                if (hasQuota && currentChar == '[' && (lastChar == '\'' || lastChar == '\"')) {
                    user_pos[1] = i;
                    proxy_user_pos[0] = i + 1;
                }

                // When user does not start with quotation marks, use the first "[" as the dividing point between user and proxy user, like xx["u2[yy]"]
                if (!hasQuota && currentChar == '[' && proxy_user_pos[0] == 0) {
                    user_pos[1] = i ;
                    proxy_user_pos[0] = i + 1;
                }

                if (currentChar == ']' && proxy_user_pos[0] != 0) {
                    proxy_user_pos[1] = i ;
                    left_info_pos = i;
                }

                lastChar = currentChar;
            }

            if (proxy_user_pos[0] != 0 && proxy_user_pos[1] >= proxy_user_pos[0]) {
                this.user = username.substring(user_pos[0], user_pos[1]) + username.substring(Math.min(left_info_pos + 1, len));
                this.proxyUser = username.substring(proxy_user_pos[0], proxy_user_pos[1]);
            } else {
                this.user = originUsername;
                this.proxyUser = "";
            }

        } catch (Exception e) {
            this.user = originUsername;
            this.proxyUser = "";
        }
    }
}
