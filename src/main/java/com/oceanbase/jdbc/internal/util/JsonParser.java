/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2022 OceanBase.
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
package com.oceanbase.jdbc.internal.util;

import java.io.InvalidObjectException;
import java.util.*;

public final class JsonParser {

    private final String input;
    private int pos = 0;
    private final Deque<Object> stack = new ArrayDeque<>();

    public JsonParser(String input) {
        this.input = input;
    }

    public void parse() throws InvalidObjectException {
        if (! parseValue()) {
            throw new InvalidObjectException("Parsing json string failed.");
        }
    }

    private void skipWhitespace() {
        while (pos < input.length() && (input.charAt(pos) == ' ' || input.charAt(pos) == '\t' || input.charAt(pos) == '\n'))
            ++pos;
    }

    private boolean parseChar(char c) {
        if (pos >= input.length()) return false;
        boolean success = input.charAt(pos) == c;
        if (!success) return false;
        ++pos;
        skipWhitespace();
        return true;
    }

    // VALUE ::= STRINGLIT / NUMBER / OBJECT / ARRAY
    public boolean parseValue() {
        return parseString() || parseNumber() || parseObject() || parseArray() || parseBoolAndNull();
    }

    private boolean parseString() {
        if (pos >= input.length()) return false;
        if (input.charAt(pos) != '"')
            return false;
        int last = input.substring(pos + 1).indexOf('"');
        if (last < 0)
            return false;
        stack.push(input.substring(pos + 1, pos + last + 1));
        pos += last + 2;
        skipWhitespace();
        return true;
    }

    private boolean parseNumber() {
        if (pos >= input.length()) return false;
        Scanner scanner = new Scanner(input.substring(pos));
        String num = scanner.useDelimiter("[^0-9]").next();
        if (num.length() > 0) {
            stack.push(Integer.parseInt(num));
            pos += num.length();
            skipWhitespace();
            return true;
        }
        return false;
    }

    private boolean parseBoolAndNull() {
        if (pos >= input.length()) return false;
        if (input.startsWith("true", pos)) {
            stack.push(true);
            pos += 4;
        } else if (input.startsWith("false", pos)) {
            stack.push(false);
            pos += 5;
        } else if (input.startsWith("null", pos)) {
            stack.push(null);
            pos += 4;
        } else {
            return false;
        }
        skipWhitespace();
        return true;
    }

    // OBJECT ::= "{" (PAIR ("," PAIR)* )? "}"
    private boolean parseObject() {
        int pos0 = pos;
        int stack0 = stack.size();
        boolean success = parseChar('{') && parsePairs() && parseChar('}');
        if (!success) {
            pos = pos0;
            return false;
        }
        HashMap<String, Object> object = new HashMap<>();
        while (stack.size() > stack0) {
            Object value = stack.pop();
            String string = (String) stack.pop();
            object.put(string, value);
        }
        stack.push(object);
        return true;
    }

    // (PAIR ("," PAIR)* )?
    private boolean parsePairs() {
        if (parsePair()) parsePairTails();
        return true;
    }

    // ("," PAIR)*
    private boolean parsePairTails() {
        while (true) {
            int pos0 = pos;
            boolean success = parseChar(',') && parsePair();
            if (!success) {
                pos = pos0;
                return true;
            }
        }
    }

    // PAIR  ::= STRINGLIT ":" VALUE
    private boolean parsePair() {
        int pos0 = pos;
        boolean success = parseString() && parseChar(':') && parseValue();
        if (!success) pos = pos0;
        return success;
    }

    // ARRAY ::= "[" (VALUE ("," VALUE)* )? "]"
    private boolean parseArray() {
        int pos0 = pos;
        int stack0 = stack.size();
        boolean success = parseChar('[') && parseValues() && parseChar(']');
        if (!success) {
            pos = pos0;
            return false;
        }
        ArrayList<Object> array = new ArrayList<>();
        while (stack.size() > stack0)
            array.add(stack.pop());
        Collections.reverse(array);
        stack.push(array);
        return true;
    }

    // (VALUE ("," VALUE)* )?
    private boolean parseValues() {
        if (parseValue()) parseValueTails();
        return true;
    }

    // ("," VALUE)*
    private boolean parseValueTails() {
        while (true) {
            int pos0 = pos;
            boolean success = parseChar(',') && parseValue();
            if (!success) {
                pos = pos0;
                return true;
            }
        }
    }

    public Deque<Object> getStack() {
        return stack;
    }

}