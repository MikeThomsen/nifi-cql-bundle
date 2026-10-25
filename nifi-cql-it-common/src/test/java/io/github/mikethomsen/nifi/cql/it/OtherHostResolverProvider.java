/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.mikethomsen.nifi.cql.it;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Gives the test JVM one extra hostname, {@value #OTHER_HOST}, that resolves to wherever {@code localhost} does.
 * Every other lookup goes to the platform's own resolver unchanged.
 *
 * <p>A secure test needs a name that reaches the test container but that the server certificate does not
 * carry, to show the hostname check rejecting it. Any second real name for the container would depend on the
 * operating system - a second loopback address only exists on Linux, and a machine's own hostname does not
 * always resolve - whereas this works anywhere the container is published on {@code localhost}. The name sits
 * under the reserved {@code .invalid} domain, so it can never collide with a real host.
 *
 * <p>Registered through {@code META-INF/services}, which the JVM reads once, at its first name lookup.
 */
public final class OtherHostResolverProvider extends InetAddressResolverProvider {

    /** A name for {@code localhost} that no certificate in these tests is issued for. */
    public static final String OTHER_HOST = "cql-other-host.invalid";

    @Override
    public InetAddressResolver get(final Configuration configuration) {
        final InetAddressResolver builtin = configuration.builtinResolver();

        return new InetAddressResolver() {
            @Override
            public Stream<InetAddress> lookupByName(final String host, final LookupPolicy lookupPolicy) throws UnknownHostException {
                if (!OTHER_HOST.equalsIgnoreCase(host)) {
                    return builtin.lookupByName(host, lookupPolicy);
                }

                // Relabelled, not passed through: the builtin resolver's answers carry the name "localhost",
                // and the driver checks the certificate against whatever name the address carries.
                final List<InetAddress> relabelled = new ArrayList<>();
                for (final InetAddress address : builtin.lookupByName("localhost", lookupPolicy).toList()) {
                    relabelled.add(InetAddress.getByAddress(OTHER_HOST, address.getAddress()));
                }
                return relabelled.stream();
            }

            @Override
            public String lookupByAddress(final byte[] address) throws UnknownHostException {
                return builtin.lookupByAddress(address);
            }
        };
    }

    @Override
    public String name() {
        return "CQL integration test resolver for " + OTHER_HOST;
    }
}
