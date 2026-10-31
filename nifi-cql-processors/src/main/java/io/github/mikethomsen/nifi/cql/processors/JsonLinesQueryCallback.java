/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the NOTICE file distributed with this work for copyright ownership.
 */
package io.github.mikethomsen.nifi.cql.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.Record;
import io.github.mikethomsen.nifi.cql.api.service.CQLQueryCallback;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.github.mikethomsen.nifi.cql.processors.AbstractCQLProcessor.REL_SUCCESS;
import static io.github.mikethomsen.nifi.cql.processors.ExecuteCQL.REL_ORIGINAL;

/**
 * Writes a result set to FlowFiles as newline-delimited JSON, one object per row.
 *
 * <p>A FlowFile is closed once it holds {@code rowsPerFlowFile} rows (0 for no limit). Closed FlowFiles are held
 * until {@code minQueued} of them exist, then released together and the session committed, so output reaches
 * downstream while the result set is still being read. With {@code minQueued} of 0, nothing is released until
 * the last row has been read.
 */
final class JsonLinesQueryCallback implements CQLQueryCallback {

    static final String MIME_TYPE = "application/x-ndjson";

    private final ProcessSession session;
    private final ComponentLog logger;
    private final long rowsPerFlowFile;
    private final int minQueued;
    private final String fragmentId = UUID.randomUUID().toString();
    private final List<FlowFile> queued = new ArrayList<>();
    private final Map<String, String> inherited;

    private FlowFile parent;
    private FlowFile current;
    private OutputStream currentStream;
    private long currentRowCount;
    private int fragmentIndex;
    private long queuedRowCount;
    private long rowsReleased;
    private long rowsReceived;

    JsonLinesQueryCallback(final FlowFile parent, final ProcessSession session, final ComponentLog logger,
                           final long rowsPerFlowFile, final int minQueued) {
        this.parent = parent;
        this.inherited = ResultAttributes.inheritedFrom(parent);
        this.session = session;
        this.logger = logger;
        this.rowsPerFlowFile = rowsPerFlowFile;
        this.minQueued = minQueued;
    }

    /** @return the rows that have already been released downstream, and so cannot be recalled */
    long rowsReleased() {
        return rowsReleased;
    }

    /** @return whether the incoming FlowFile has already been sent on, which only happens once output is released */
    @Override
    public boolean hasSentOriginal() {
        return parent == null;
    }

    /** @return whether any row was received, so any FlowFile was created */
    boolean isEmpty() {
        return rowsReceived == 0;
    }

    @Override
    public void receive(final long rowNumber, final Record result, final boolean hasMore) {
        rowsReceived++;

        try {
            if (current == null) {
                current = session.putAllAttributes(session.create(), inherited);
                currentStream = session.write(current);
                currentRowCount = 0;
            }

            currentStream.write((RecordJson.toJson(result) + "\n").getBytes(StandardCharsets.UTF_8));
            currentRowCount++;

            final boolean full = rowsPerFlowFile > 0 && currentRowCount >= rowsPerFlowFile;
            if (full || !hasMore) {
                closeCurrent();
            }

            if (!hasMore) {
                releaseQueued(false);
            } else if (minQueued > 0 && queued.size() >= minQueued) {
                releaseQueued(true);
            }
        } catch (final IOException | RuntimeException e) {
            clear();
            throw new ProcessException("Error writing row " + rowNumber + " as JSON", e);
        }
    }

    private void closeCurrent() throws IOException {
        currentStream.close();
        currentStream = null;

        current = session.putAllAttributes(current, Map.of(
                "mime.type", MIME_TYPE,
                "record.count", String.valueOf(currentRowCount),
                FragmentAttributes.FRAGMENT_ID.key(), fragmentId,
                FragmentAttributes.FRAGMENT_INDEX.key(), String.valueOf(fragmentIndex++)));

        queued.add(current);
        queuedRowCount += currentRowCount;
        current = null;
    }

    /**
     * Sends every queued FlowFile to success and the incoming FlowFile to original. {@code fragment.count}
     * is only knowable, and so only set, when nothing was released earlier.
     */
    private void releaseQueued(final boolean commit) {
        if (!commit && rowsReleased == 0) {
            final String total = String.valueOf(queued.size());
            queued.replaceAll(flowFile -> session.putAttribute(flowFile, FragmentAttributes.FRAGMENT_COUNT.key(), total));
        }

        // Lineage can only name the incoming FlowFile while it is still in the session, so FlowFiles released after
        // it, when Minimum FlowFiles Queued sends output early, carry its attributes but no link to it.
        if (parent != null) {
            session.getProvenanceReporter().fork(parent, queued);
        }
        session.transfer(queued, REL_SUCCESS);
        rowsReleased += queuedRowCount;
        queuedRowCount = 0;
        queued.clear();

        if (parent != null) {
            session.transfer(parent, REL_ORIGINAL);
            parent = null;
        }

        if (commit) {
            session.commitAsync();
        }
    }

    /**
     * Discards everything not yet released. A stream open on a FlowFile must be closed first, because the
     * session refuses to remove a FlowFile that is still being written.
     */
    @Override
    public void clear() {
        if (currentStream != null) {
            try {
                currentStream.close();
            } catch (final IOException e) {
                logger.warn("Error closing output stream while discarding a partial result", e);
            }
            currentStream = null;
        }

        if (current != null) {
            session.remove(current);
            current = null;
        }

        session.remove(queued);
        queued.clear();
        queuedRowCount = 0;
    }
}
