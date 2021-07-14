/*
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/**
 * @test
 * @bug 8080679 8131913
 * @modules jdk.internal.le/jdk.internal.jline.internal
 * @summary Verify ConsoleReader.stripAnsi strips escape sequences from its input correctly.
 */

import jdk.internal.jline.internal.Ansi;

public class StripAnsiTest {
    public static void main(String... args) throws Exception {
        new StripAnsiTest().run();
    }

    void run() throws Exception {
        String withAnsi = "0\033[s1\033[2J2\033[37;4m3";
        String expected = "0123";

        String actual = Ansi.stripAnsi(withAnsi);

        if (!expected.equals(actual)) {
            throw new IllegalStateException("Did not correctly strip escape sequences: " + actual);
        }
    }
}
