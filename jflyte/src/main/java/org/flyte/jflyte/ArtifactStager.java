/*
 * Copyright 2021 Flyte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.flyte.jflyte;

import static com.google.common.base.Verify.verify;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.Manifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Incrementally copies files into {@link FileSystem}. Adds hashes to file names to avoid naming
 * collisions.
 *
 * <p>Implements staging compatible with DataflowRunner staging algorithm not to create unnecessary
 * copies of files. The only way of doing staging for now.
 *
 * <p>For now, we can only stage files, if file list starts to get out of hand, we can add support
 * for directories as well, where directory would be unique and contain hash code of all of it's
 * contents.
 */
class ArtifactStager {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactStager.class);

  private final String stagingLocation;
  private final FileSystem fileSystem;

  ArtifactStager(String stagingLocation, FileSystem fileSystem) {
    this.stagingLocation = stagingLocation;
    this.fileSystem = fileSystem;
  }

  static ArtifactStager create(Config config, Collection<ClassLoader> modules) {
    try {
      String stagingLocation = config.stagingLocation();

      if (stagingLocation == null) {
        throw new IllegalArgumentException(
            "Environment variable 'FLYTE_STAGING_LOCATION' isn't set");
      }

      URI stagingUri = new URI(stagingLocation);
      Map<String, FileSystem> fileSystems = FileSystemLoader.loadFileSystems(modules);
      FileSystem stagingFileSystem = FileSystemLoader.getFileSystem(fileSystems, stagingUri);

      return new ArtifactStager(stagingLocation, stagingFileSystem);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to parse stagingLocation", e);
    }
  }

  List<Artifact> stageFiles(List<String> files) {
    List<Artifact> artifacts = new ArrayList<>();

    // TODO use multiple threads for better throughput
    for (String filePath : files) {
      File file = new File(filePath);

      verify(file.exists(), "file doesn't exist [%s]", filePath);
      verify(!file.isDirectory(), "directories aren't supported [%s]", filePath);

      Artifact artifact = getArtifactForFile(file, stagingLocation);
      stageArtifact(artifact, Files.asByteSource(file));

      artifacts.add(artifact);
    }

    return artifacts;
  }

  void stageArtifact(Artifact artifact, ByteSource content) {
    LOG.info("Staging [{}] to [{}]", artifact.name(), artifact.location());

    Manifest manifest = fileSystem.getManifest(artifact.location());
    if (manifest == null) {
      // TODO writer API should accept crc32c as an option to pass it to underlying implementation
      // that is going to double-check it once blob is uploaded

      try (WritableByteChannel writer = fileSystem.writer(artifact.location())) {
        content.copyTo(Channels.newOutputStream(writer));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      // TODO check that crc32c matches
    }
  }

  private static String getLocation(String stagingLocation, String fileName, String hash) {
    String nameWithoutExtension = Files.getNameWithoutExtension(fileName);
    String fileExtension = Files.getFileExtension(fileName);

    String fileNameWithHash = nameWithoutExtension + "-" + hash + "." + fileExtension;

    if (stagingLocation.endsWith("/")) {
      return stagingLocation + fileNameWithHash;
    } else {
      return stagingLocation + "/" + fileNameWithHash;
    }
  }

  Artifact getArtifact(String name, ByteSource bs) {
    return getArtifact(name, bs, stagingLocation);
  }

  static Artifact getArtifactForFile(File file, String stagingLocation) {
    return getArtifact(file.getName(), Files.asByteSource(file), stagingLocation);
  }

  static Artifact getArtifact(String name, ByteSource bs, String stagingLocation) {
    // md5 is fine, don't change because of compatibility
    @SuppressWarnings({"deprecation"})
    Hasher hasher = Hashing.md5().newHasher();
    OutputStream os = Funnels.asOutputStream(hasher);
    long size;

    try (CountingOutputStream cos = new CountingOutputStream(os)) {
      bs.copyTo(cos);

      cos.flush();
      size = cos.getCount();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    String hash = BaseEncoding.base64Url().omitPadding().encode(hasher.hash().asBytes());
    String location =
        getLocation(/* stagingLocation= */ stagingLocation, /* fileName= */ name, /* hash= */ hash);

    return Artifact.create(/* location= */ location, /* name= */ name, /* size= */ size);
  }
}
