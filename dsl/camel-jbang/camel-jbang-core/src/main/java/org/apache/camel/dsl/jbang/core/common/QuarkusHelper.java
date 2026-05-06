/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.dsl.jbang.core.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.xml.sax.InputSource;

import org.apache.camel.tooling.maven.MavenArtifact;
import org.apache.camel.tooling.maven.MavenDownloader;
import org.apache.camel.tooling.maven.MavenResolutionException;
import org.apache.camel.util.json.JsonArray;
import org.apache.camel.util.json.JsonObject;
import org.apache.camel.util.json.Jsoner;
import org.apache.maven.artifact.versioning.ComparableVersion;

/**
 * Helper for resolving Quarkus platform information from the Quarkus registry.
 */
public final class QuarkusHelper {

    public static final String QUARKUS_PLATFORM_URL_PROPERTY = "camel.jbang.quarkus.platform.url";
    public static final String DEFAULT_QUARKUS_PLATFORM_URL = RuntimeType.QUARKUS_EXTENSION_REGISTRY_BASE_URL
                                                              + (RuntimeType.QUARKUS_EXTENSION_REGISTRY_BASE_URL.endsWith("/")
                                                                      ? "" : "/")
                                                              + "client/platforms";

    private QuarkusHelper() {
    }

    /**
     * Returns the Quarkus platform registry URL, honoring the system property {@value #QUARKUS_PLATFORM_URL_PROPERTY}
     * if set.
     */
    public static String getQuarkusPlatformUrl() {
        return System.getProperty(QUARKUS_PLATFORM_URL_PROPERTY, DEFAULT_QUARKUS_PLATFORM_URL);
    }

    /**
     * Resolves the actual Quarkus platform version for each row by fetching the Quarkus platform registry and matching
     * the Camel Quarkus major.minor version against stream IDs.
     *
     * @param rows                 the list of rows to enrich with Quarkus platform versions
     * @param runtimeVersionFunc   function to extract the runtime (Camel Quarkus) version from a row
     * @param quarkusVersionSetter consumer to set the resolved Quarkus platform version on a row
     */
    public static <T> void resolveQuarkusPlatformVersions(
            List<T> rows,
            Function<T, String> runtimeVersionFunc,
            BiConsumer<T, String> quarkusVersionSetter) {

        JsonArray streams = fetchPlatformStreams();
        if (streams == null) {
            return;
        }

        // keep the row with the highest runtime version per major.minor stream
        BinaryOperator<T> keepLatest = (a, b) -> VersionHelper.compare(
                runtimeVersionFunc.apply(a), runtimeVersionFunc.apply(b)) >= 0 ? a : b;

        Map<String, T> latestPerStream = rows.stream()
                .filter(row -> runtimeVersionFunc.apply(row) != null)
                .collect(Collectors.toMap(
                        row -> VersionHelper.getMajorMinorVersion(runtimeVersionFunc.apply(row)),
                        Function.identity(),
                        keepLatest));

        // match each major.minor against registry streams and set the quarkus version
        latestPerStream.forEach((majorMinor, row) -> findStreamVersion(streams, majorMinor, "quarkus-core-version")
                .ifPresent(version -> quarkusVersionSetter.accept(row, version)));
    }

    /**
     * Finds the newest Quarkus platform BOM version using the same or newer {@code major.minor} Camel version as the
     * specified {@code camelVersion} by searching in Quarkus platform registry or returns the specified
     * {@code buildTimeQuarkusVersion} if no compatible Platform version can be found.
     * <p>
     * This is used by export/run commands to query the registry for the correct platform BOM version instead of using
     * the build-time constant. The registry may have a newer compatible version.
     *
     * @param  buildTimeQuarkusVersion the build-time Quarkus version (e.g., "3.15.7.something")
     * @param  camelVersion            if specified, the value of {@code --camel-version} CLI parameter or the Camel
     *                                 version of the currently running camel-jbang. Must not be {@code null}
     * @return                         the resolved platform BOM version from the registry, or the original
     *                                 buildTimeVersion if resolution fails
     */
    public static String resolveQuarkusPlatformVersion(
            String buildTimeQuarkusVersion, String camelVersion, MavenDownloader downloader, Set<String> repos) {
        Objects.requireNonNull(camelVersion, "camelVersion");

        JsonArray streams = fetchPlatformStreams();
        if (streams == null) {
            return buildTimeQuarkusVersion;
        }
        MavenResolver resolver = new MavenResolver(downloader, repos);
        Optional<String> resolved
                = findPlatformVersion(streams, new MajorMinor(camelVersion), resolver::resolve);
        return resolved.orElse(buildTimeQuarkusVersion);
    }

    static record MavenResolver(MavenDownloader downloader, Set<String> repos) {
        Path resolve(String resolverGatv) {
            List<MavenArtifact> artifacts;
            try {
                artifacts = downloader.resolveArtifacts(List.of(resolverGatv), repos, false, false);
                if (artifacts.size() != 1) {
                    throw new IllegalStateException("Could not resolve " + resolverGatv);
                }
            } catch (MavenResolutionException e) {
                throw new RuntimeException("Could not resolve " + resolverGatv, e);
            }
            return artifacts.get(0).getFile().toPath();
        }
    }

    /**
     * Fetches the platform streams array from the Quarkus platform registry.
     *
     * @return the streams JsonArray, or null if the registry is unreachable or the response is invalid
     */
    private static JsonArray fetchPlatformStreams() {
        try {
            HttpClient hc = HttpClient.newHttpClient();
            HttpResponse<String> res = hc.send(
                    HttpRequest.newBuilder(new URI(getQuarkusPlatformUrl()))
                            .timeout(Duration.ofSeconds(2))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            if (res.statusCode() == 200) {
                JsonObject json = (JsonObject) Jsoner.deserialize(res.body());
                JsonArray platforms = json.getCollection("platforms");
                if (platforms == null || platforms.isEmpty()) {
                    return null;
                }
                JsonObject platform = platforms.getMap(0);
                return platform.getCollection("streams");
            }
        } catch (Exception e) {
            // ignore - if the registry is not reachable within 2 seconds, return null
        }
        return null;
    }

    /**
     * Finds a version field from the registry streams that matches the given major.minor stream ID.
     *
     * @param  streams    the streams array from the registry
     * @param  majorMinor the major.minor version to match (e.g., "3.15")
     * @param  fieldName  the field name to extract from the release ("version" or "quarkus-core-version")
     * @return            the version string, or empty if not found
     */
    private static Optional<String> findStreamVersion(JsonArray streams, String majorMinor, String fieldName) {
        return streams.stream()
                .map(s -> (JsonObject) s)
                .filter(stream -> majorMinor.equals(stream.getString("id")))
                .findFirst()
                .map(stream -> (JsonArray) stream.getCollection("releases"))
                .filter(releases -> !releases.isEmpty())
                .map(releases -> (JsonObject) releases.getMap(0))
                .map(release -> release.getString(fieldName));
    }

    /**
     *
     * @param  streams    the streams array from the registry
     * @param  majorMinor the major.minor version to match (e.g., "3.15")
     * @param  fieldName  the field name to extract from the release ("version" or "quarkus-core-version")
     * @return            the version string, or empty if not found
     */
    static Optional<String> findPlatformVersion(
            JsonArray streams, MajorMinor wantedCamelVersion, Function<String, Path> mavenResolver) {
        return streams.stream()
                .map(s -> (JsonObject) s)
                .map(PlatformStream::of)
                .map(platformStream -> platformStream.platformReleases().stream().findFirst().orElse(null))
                .filter(platformRelease -> platformRelease != null)
                // stream of PlatformReleases having a quarkus-camel-bom
                .map(platformRelease -> CamelVersionInPlatformRelease.of(platformRelease, mavenResolver,
                        wantedCamelVersion))
                .sorted()
                .map(camelInPlatform -> camelInPlatform.platformVersion().toString())
                .findFirst();
    }

    private record PlatformStream(String id, List<PlatformRelease> platformReleases) {
        static PlatformStream of(JsonObject json) {
            List<PlatformRelease> releases = json.getCollectionOrDefault("releases", List.of()).stream()
                    .map(o -> (JsonObject) o)
                    .map(PlatformRelease::of)
                    .toList();
            return new PlatformStream(Objects.requireNonNull(json.getString("id"), "stream.id"), List.copyOf(releases));
        }
    }

    private record PlatformRelease(ComparableVersion platformVersion, String quarkusCamelBomGav) {
        static PlatformRelease of(JsonObject json) {
            String quarkusCamelBomGav = json.getCollectionOrDefault("member-boms", List.of()).stream()
                    .map(o -> (String) o)
                    .filter(gav -> gav.contains(":quarkus-camel-bom:"))
                    .findFirst()
                    .orElse(null);
            return new PlatformRelease(
                    new ComparableVersion(Objects.requireNonNull(json.getString("version"), "release.version")),
                    quarkusCamelBomGav);
        }
    }

    record CamelVersionInPlatformRelease(MajorMinor camelVersion, BigInteger versionDistance,
            ComparableVersion platformVersion) implements Comparable<CamelVersionInPlatformRelease> {

        private static final Comparator<CamelVersionInPlatformRelease> COMPARATOR
                = Comparator.comparing((CamelVersionInPlatformRelease rel) -> rel.versionDistance())
                        .thenComparing(CamelVersionInPlatformRelease::platformVersion, Comparator.reverseOrder());

        static CamelVersionInPlatformRelease of(
                PlatformRelease platformRelease,
                Function<String, Path> mavenResolver,
                MajorMinor wantedCamelVersion) {
            // the platform uses the format g:a:[c]:t:v, while the downloader expects g:a:[t[:c]]:v
            final String resolverGatv = platformRelease.quarkusCamelBomGav().replace("::pom:", ":pom:");
            Path file = mavenResolver.apply(resolverGatv);
            if (!Files.isRegularFile(file)) {
                throw new IllegalStateException(file + " should exist for " + resolverGatv);
            }
            XPath xPath = XPathFactory.newInstance().newXPath();
            String expr = anyNs("project", "dependencyManagement", "dependencies", "dependency")
                          + "[*[local-name()='groupId']/text()='org.apache.camel' and *[local-name()='artifactId']/text()='camel-direct']"
                          + anyNs("version")
                          + "/text()";
            try (InputStream in = Files.newInputStream(file)) {

                String camelVersion = (String) xPath.evaluate(expr, new InputSource(in), XPathConstants.STRING);
                MajorMinor comparableCamelVersion = new MajorMinor(camelVersion);
                BigInteger dist = comparableCamelVersion.distanceTo(wantedCamelVersion);

                return new CamelVersionInPlatformRelease(
                        comparableCamelVersion, dist, platformRelease.platformVersion);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not read " + file, e);
            } catch (XPathExpressionException e) {
                throw new RuntimeException("Could not evaluate " + expr + " on file " + file);
            }
        }

        /**
         * A generator of XPath 1.0 "any namespace" selector, such as
         * {@code /*[local-name()='foo']/*[local-name()='bar']}. In XPath 2.0, this would be just {@code /*:foo/*:bar},
         * but as of Java 25, there is only XPath 1.0 available in the JDK so we have to use this workaround.
         *
         * @param  elements namespace-less element names
         * @return          am XPath 1.0 style selector
         */
        static String anyNs(String... elements) {
            StringBuilder sb = new StringBuilder();
            for (String e : elements) {
                sb.append("/*[local-name()='").append(e).append("']");
            }
            return sb.toString();
        }

        @Override
        public int compareTo(CamelVersionInPlatformRelease o) {
            return COMPARATOR.compare(this, o);
        }
    }

    static class MajorMinor {
        private final String source;
        private final int major;
        private final int minor;
        private final int majorMinor;

        public MajorMinor(String version) {
            this.source = Objects.requireNonNull(version, "version");
            String[] segments = version.split("\\.");
            this.major = segments.length >= 1 ? Integer.parseInt(segments[0]) : 0;
            this.minor = segments.length >= 2 ? Integer.parseInt(segments[1]) : 0;
            if ((major & 0xFFFF0000) != 0) {
                throw new IllegalArgumentException("Cannot handle major longer than 16 bits; found" + major);
            }
            if ((minor & 0xFFFF0000) != 0) {
                throw new IllegalArgumentException("Cannot handle minor longer than 16 bits; found" + minor);
            }
            this.majorMinor = (major << 16) | minor;
        }

        @Override
        public String toString() {
            return source;
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof MajorMinor) && source.equals(((MajorMinor) o).source);
        }

        public int getMajor() {
            return major;
        }

        public int getMinor() {
            return minor;
        }

        @Override
        public int hashCode() {
            return source.hashCode();
        }

        /**
         * A distance between this {@link MajorMinor} and other {@link MajorMinor} can be used as a key for ordering.
         *
         * The returned value of each of the following version groups is guaranteed to be larger than the one of the
         * previous group.
         * <ol>
         * <li>perfect match -> 0
         * <li>same major, newer other minor, closer is higher
         * <li>same major, older other minor, newer is higher
         * <li>other newer major, closer is higher
         * <li>other older major, newer is higher
         * </ol>
         * Example: for {@code thisVersion} 2.3, the distances computed for the following {@code otherVersion} would
         * give the following ordering:
         * <ul>
         * <li>2.3 (perfect match)
         * <li>2.4 (same major, closest newer minor)
         * <li>2.5 (same major, further newer minor)
         * <li>2.2 (same major, older minor)
         * <li>2.1 (same major, even older minor)
         * <li>3.0 (closest newer major)
         * <li>3.1 (less close newer major)
         * <li>4.0 (even less close newer major)
         * <li>4.1 (even less close newer major)
         * <li>1.2 (closest older major)
         * <li>1.1 (less close older major)
         * <li>0.2 (even less close older major)
         * <li>0.1 (even less close older major)
         * </ul>
         *
         * @param  thisVersion
         * @param  otherVersion
         * @return
         */
        BigInteger distanceTo(MajorMinor other) {
            if (this.equals(other)) {
                return BigInteger.ZERO;
            }

            int distance = other.majorMinor - majorMinor;
            if (major == other.major) {
                // if major is the same, then distance has maximum length of 16 bits
                if (minor <= other.minor) {
                    // case 1: same major, newer minor
                    // occupy the right most 16 bytes
                    return BigInteger.valueOf(distance);
                } else {
                    // case 2: same major, older minor
                    // make it more distant by shifting left by 16 bits
                    // we occupy the right most 32 bytes (16 bit value shifted by 16)
                    return BigInteger.valueOf(Math.abs(distance)).shiftLeft(16);
                }
            } else if (major < other.major) {
                // case 3: newer major
                // make it more distant than cases 1 and 2 by shifting left
                // we have to shift by 32 so that we do not clash with cases 1 and 2
                // we occupy the right most 64 bytes (32 bit value shifted by 32)
                return BigInteger.valueOf(distance).shiftLeft(32);
            } else {
                // case 4: older major
                // make it more distant than cases 1, 2 and 3 by shifting left
                // we have to shift by 64 so that we do not clash with cases 1, 2 and 3
                // we occupy the right most 80 bytes (32 bit value shifted by 64) a long value would not be enough
                return BigInteger.valueOf(Math.abs(distance)).shiftLeft(64);
            }
        }
    }
}
