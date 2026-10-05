/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SliceIndexingTests extends ESTestCase {

    public void testValidateUserSliceValueAcceptsSafeValues() {
        SliceIndexing.validateUserSliceValue("s1");
        SliceIndexing.validateUserSliceValue("tenant-1");
        SliceIndexing.validateUserSliceValue("tenant.group:01");
        SliceIndexing.validateUserSliceValue("SOME_SLICE");
    }

    public void testValidateUserSliceValueRejectsReservedValue() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.validateUserSliceValue(SliceIndexing.SLICE_ALL)
        );
        assertThat(ex.getMessage(), containsString("invalid [slice] value"));
        assertThat(ex.getMessage(), containsString("reserved"));
    }

    public void testValidateUserSliceValueRejectsIllegalCharacters() {
        assertInvalid("slice,1");
        assertInvalid("slice*1");
        assertInvalid("slice?1");
        assertInvalid("slice 1");
        assertInvalid(".slice1");
        assertInvalid("-slice1");
        assertInvalid("_slice1");
        assertInvalid("slice1.");
        assertInvalid("slice1-");
        assertInvalid("slice1_");
    }

    public void testValidateUserSliceValueRejectsEmptyAndTooLong() {
        assertInvalid("");
        assertInvalid("a".repeat(129));
    }

    public void testParseRoutingOrSliceReturnsRoutingWhenSliceAbsent() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("routing", "r1")).build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("r1"));
        assertThat(parsed.fromSlice(), equalTo(false));
    }

    public void testParseRoutingOrSliceReturnsSliceWhenPresent() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("slice", "s1")).build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("s1"));
        assertThat(parsed.fromSlice(), equalTo(true));
    }

    public void testParseRoutingOrSliceRejectsWhenBothPresent() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("routing", "r1", "slice", "s1")).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("[routing] is not allowed together with [slice]"));
    }

    public void testParseRoutingOrSliceRejectsSliceWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("slice", "s1")).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("request does not support [slice]"));
    }

    public void testToSearchSlice() {
        assertNull(SliceIndexing.toSearchSlice("r1", false));
        assertNull(SliceIndexing.toSearchSlice(null, false));
        assertThat(SliceIndexing.toSearchSlice("s1", true), equalTo("s1"));
        assertThat(SliceIndexing.toSearchSlice(null, true), equalTo(SliceIndexing.SLICE_ALL));
    }

    public void testSliceToRouting() {
        assertThat(SliceIndexing.sliceToRouting("s1"), equalTo("s1"));
        assertThat(SliceIndexing.sliceToRouting("s1,s2"), equalTo("s1,s2"));
        assertNull(SliceIndexing.sliceToRouting(SliceIndexing.SLICE_ALL));
    }

    public void testPrefixTermPutsTheSliceFirst() {
        BytesRef prefixed = SliceIndexing.prefixTerm("tenant-1", new BytesRef("error"));
        assertThat(prefixed.utf8ToString(), equalTo("tenant-1|error"));
    }

    public void testStripTermPrefixRoundTrips() {
        for (String slice : new String[] { "a", "tenant-1", "A.b_c:d-9", randomAlphaOfLength(128) }) {
            for (String term : new String[] { "", "x", "error", "has|a|separator", randomUnicodeOfLength(20) }) {
                BytesRef term0 = new BytesRef(term);
                BytesRef roundTripped = SliceIndexing.stripTermPrefix(SliceIndexing.prefixTerm(slice, term0));
                assertThat("slice [" + slice + "] term [" + term + "]", roundTripped, equalTo(term0));
            }
        }
    }

    /**
     * A term may contain the separator, so stripping must cut at the first one. Slice values cannot contain it, which is what makes
     * the first occurrence unambiguous.
     */
    public void testStripCutsAtTheFirstSeparator() {
        BytesRef prefixed = SliceIndexing.prefixTerm("s1", new BytesRef("a|b"));
        assertThat(prefixed.utf8ToString(), equalTo("s1|a|b"));
        assertThat(SliceIndexing.stripTermPrefix(prefixed).utf8ToString(), equalTo("a|b"));
    }

    public void testSeparatorIsNotAValidSliceCharacter() {
        String separator = String.valueOf((char) SliceIndexing.SLICE_TERM_SEPARATOR);
        assertInvalid(separator);
        assertInvalid("a" + separator + "b");
    }

    /**
     * Distinct slices never produce the same prefixed term, which is what keeps their postings disjoint. This holds only because a
     * slice value cannot contain the separator: {@code prefixTerm("a", "b|c")} and {@code prefixTerm("a|b", "c")} would both be
     * {@code a|b|c}, and it is {@link #testSeparatorIsNotAValidSliceCharacter} that rules the second one out.
     */
    public void testPrefixedTermsAreUnambiguousAcrossValidSlices() {
        assertThat(SliceIndexing.prefixTerm("ab", new BytesRef("c")), not(equalTo(SliceIndexing.prefixTerm("a", new BytesRef("bc")))));
        assertThat(SliceIndexing.prefixTerm("a", new BytesRef("b")), not(equalTo(SliceIndexing.prefixTerm("b", new BytesRef("a")))));
        // A term may contain the separator without creating ambiguity, precisely because the slice cannot.
        assertThat(SliceIndexing.prefixTerm("a", new BytesRef("b|c")), not(equalTo(SliceIndexing.prefixTerm("ab", new BytesRef("c")))));
    }

    /** Prefixing preserves term order within a slice, which is what makes range and prefix queries translate unchanged. */
    public void testPrefixingPreservesOrderWithinASlice() {
        String slice = "tenant-1";
        for (int i = 0; i < 50; i++) {
            BytesRef a = new BytesRef(randomAlphaOfLengthBetween(0, 12));
            BytesRef b = new BytesRef(randomAlphaOfLengthBetween(0, 12));
            int plain = a.compareTo(b);
            int withPrefix = SliceIndexing.prefixTerm(slice, a).compareTo(SliceIndexing.prefixTerm(slice, b));
            assertThat("a=" + a.utf8ToString() + " b=" + b.utf8ToString(), Integer.signum(withPrefix), equalTo(Integer.signum(plain)));
        }
    }

    /** A slice's terms sort together: every term of an earlier slice precedes every term of a later one. */
    public void testSlicesSortAsContiguousRuns() {
        BytesRef lastOfFirst = SliceIndexing.prefixTerm("s1", new BytesRef("zzzzzzzz"));
        BytesRef firstOfSecond = SliceIndexing.prefixTerm("s2", new BytesRef(""));
        assertTrue(lastOfFirst.compareTo(firstOfSecond) < 0);
    }

    public void testStripLeavesAnUnprefixedTermAlone() {
        BytesRef plain = new BytesRef("error");
        assertThat(SliceIndexing.stripTermPrefix(plain), equalTo(plain));
    }

    private static void assertInvalid(String value) {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> SliceIndexing.validateUserSliceValue(value));
        assertThat(ex.getMessage(), containsString("invalid [slice] value"));
    }
}
