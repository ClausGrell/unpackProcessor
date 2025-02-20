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
package grell.processors.demo;

import org.apache.maven.surefire.shared.compress.archivers.tar.TarArchiveEntry;
import org.apache.maven.surefire.shared.compress.archivers.tar.TarArchiveInputStream;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Tags({"zip tar zstd decompress"})
@CapabilityDescription("Decompress files")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class Unpack extends AbstractProcessor {

    public static final PropertyDescriptor METHOD = new PropertyDescriptor
            .Builder().name("METHOD")
            .displayName("Method")
            .description("COMPRESS or DECOMPRESS")
            .required(true)
            .allowableValues("COMPRESS", "DECOMPRESS")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success")
            .build();

    public static final Relationship RELATIONSHIP_FALIURE = new Relationship.Builder()
            .name("Failure")
            .description("Failure")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(METHOD);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(RELATIONSHIP_SUCCESS);
        relationships.add(RELATIONSHIP_FALIURE);

        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return; // No FlowFile to process
        }
        var mimeType = flowFile.getAttribute("mime.type");
        getLogger().info("Mimetype = " + mimeType);

        if (mimeType.equals("application/zstd")) {
            getLogger().info("Decompress zstdfiles" + mimeType);
            ZstdOperations zstdOperations = new ZstdOperations();
            try {
                var newFlowFile = zstdOperations.decompress(flowFile, session);
                var newFlowFile2 = zstdOperations.decompress(flowFile, session);
                session.transfer(newFlowFile, RELATIONSHIP_SUCCESS);
                session.transfer(newFlowFile2, RELATIONSHIP_SUCCESS);
                session.remove(flowFile);
            } catch (IOException e) {
                getLogger().error("Failed to decompress the zstd content", e);
                session.transfer(flowFile, RELATIONSHIP_FALIURE); // Transfer to failure if an error occurs
            }
        } else if (mimeType.equals("application/zip")) {
            getLogger().info("Decompress zipfiles" + mimeType);
            try {
                ZipOperations zipOperations = new ZipOperations();
                InputStream inputStream = session.read(flowFile);
                ZipInputStream zstdInputStream = new ZipInputStream(inputStream);

                ZipEntry entry;
                while((entry = zstdInputStream.getNextEntry()) != null) {
                    if (!entry.isDirectory()) {
                        var newFlowFile = zipOperations.getNextFile(flowFile, entry, zstdInputStream, session);
                        session.transfer(newFlowFile, RELATIONSHIP_SUCCESS);
                    }
                }
                zstdInputStream.close();
                session.remove(flowFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }  else if (mimeType.equals("application/x-tar")) {
            getLogger().info("Decompress tarfiles" + mimeType);
            try {
                TarOperations tarOperations = new TarOperations();
                InputStream inputStream = session.read(flowFile);
                TarArchiveInputStream tarInputStream = new TarArchiveInputStream(inputStream);
                TarArchiveEntry tarArchiveEntry;
                while((tarArchiveEntry = tarInputStream.getNextEntry()) != null) {
                    if (!tarArchiveEntry.isDirectory()) {
                        var newFlowFile = tarOperations.getNextFile(flowFile, tarArchiveEntry, tarInputStream, session);
                        session.transfer(newFlowFile, RELATIONSHIP_SUCCESS);
                    }
                }
                tarInputStream.close();
                session.remove(flowFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
