/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.attachment.extract;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ExtractTextRequest extends ActionRequest implements Releasable {
    private final String name;
    private final BytesReference content;

    private final Runnable release;

    public ExtractTextRequest(StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
        final ReleasableBytesReference releasable = in.readReleasableBytesReference();
        this.release = releasable::close;
        this.content = releasable;
    }

    public ExtractTextRequest(String name, BytesReference content) {
        this.name = name;
        this.content = content;
        this.release = () -> {};
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (name == null || name.isEmpty()) {
            validationException = addValidationError("name missing", validationException);
        }

        return validationException;
    }

    public String getName() {
        return name;
    }

    public BytesReference getContent() {
        return content;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeBytesReference(content);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtractTextRequest request = (ExtractTextRequest) o;
        return Objects.equals(name, request.name) &&
            Objects.equals(content, request.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, content);
    }

    @Override
    public void close() {
        release.run();
    }
}
