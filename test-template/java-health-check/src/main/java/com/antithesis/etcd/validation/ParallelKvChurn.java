package com.antithesis.etcd.validation;

import static com.antithesis.sdk.Assert.*;
import static com.antithesis.sdk.Random.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * PARALLEL (a.k.a. "anytime") driver, written in Java.
 *
 * The Antithesis Test Composer schedules files named "parallel_*" to run at
 * ANY time during a test, and may run several in parallel, interleaved with
 * fault injection. This is the "anytime" half the evaluation asks for.
 *
 * Each invocation performs one randomized write-then-read against a randomly
 * chosen etcd node, and makes two SDK assertions:
 *
 *   - sometimes(...) : a coverage guard proving the write path actually
 *                      succeeded at least once across the whole campaign. If
 *                      Antithesis ever reports this as never-satisfied, the
 *                      driver never managed a successful write under fault,
 *                      which would itself be a finding.
 *
 *   - always(...)    : the linearizable-ish invariant that a value just written
 *                      and acknowledged by the cluster must read back equal on
 *                      the same node. If a fault sequence ever breaks this, an
 *                      acknowledged write was lost or corrupted.
 *
 * Mirrors the structure of the existing EventuallyValidation.java so the two
 * Java drivers (anytime + eventually) share one Maven module and build.
 *
 * Randomness
 * ----------
 * Every random decision goes through the Antithesis Random SDK rather than
 * java.util.Random / ThreadLocalRandom / UUID:
 *
 *   - The target node is picked with randomChoice(...), a *structured* choice.
 *     Telling Antithesis "I am choosing one of these three nodes" lets the
 *     platform steer which node each invocation hits and learn which choices
 *     surface interesting states — far more useful than hiding the decision
 *     behind nextInt(3).
 *
 *   - The unique key/value suffix is drawn with getRandom(), routing that
 *     entropy through the platform as well.
 *
 * Both values are used immediately and never stored or used to seed another
 * RNG, per Antithesis guidance (delaying or re-seeding defeats the platform's
 * ability to control execution). Outside Antithesis, both SDK methods fall back
 * to java.util.Random, so the same build runs locally and under test.
 */
public class ParallelKvChurn {

    private static final List<String> ETCD_ENDPOINTS = Arrays.asList(
        "http://etcd0:2379",
        "http://etcd1:2379",
        "http://etcd2:2379"
    );

    /**
     * Write a unique value to a unique key on the given endpoint, then read it
     * back from the same endpoint. Returns true if the round-trip value matched.
     */
    static boolean writeThenRead(String endpoint, String key, String value) throws Exception {
        Client client = Client.builder().endpoints(endpoint).build();
        try {
            KV kvClient = client.getKVClient();

            ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
            ByteSequence valueBytes = ByteSequence.from(value, StandardCharsets.UTF_8);

            // Write and wait for the cluster to acknowledge it.
            kvClient.put(keyBytes, valueBytes).get();

            // Read it straight back from the same node.
            GetResponse getResponse = kvClient.get(keyBytes).get();

            if (getResponse.getKvs().isEmpty()) {
                return false;
            }
            String retrieved =
                getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);

            System.out.println(
                "Client [parallel_kv_churn]: wrote '" + value + "' to '" + key
                    + "' on " + endpoint + ", read back '" + retrieved + "'");

            return retrieved.equals(value);
        } finally {
            client.close();
        }
    }

    public static void main(String[] args) {
        // We expect to reach this under test; lets Antithesis confirm the
        // parallel driver is actually being exercised.
        reachable("Parallel kv-churn driver started", null);

        // Structured choice: signal to Antithesis that we are selecting one of
        // the cluster nodes, and let it steer that choice. Used immediately.
        String endpoint = randomChoice(ETCD_ENDPOINTS);

        // Unique key/value for this invocation, drawn from the SDK and consumed
        // right away. Two independent draws keep key and value distinct.
        String key = "churn-" + Long.toHexString(getRandom());
        String value = "val-" + Long.toHexString(getRandom());

        ObjectMapper mapper = new ObjectMapper();

        boolean writeSucceeded = false;
        boolean valueMatched = false;
        String errorMessage = null;

        try {
            valueMatched = writeThenRead(endpoint, key, value);
            writeSucceeded = true;
        } catch (Exception e) {
            // A transient failure under fault injection (node down, partition)
            // is expected and is NOT itself a violation. We record it and let
            // the sometimes-assertion below capture coverage.
            errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
            System.out.println(
                "Client [parallel_kv_churn]: write/read attempt failed: " + errorMessage);
        }

        // --- sometimes: coverage guard for the anytime write path ---
        ObjectNode wroteDetails = mapper.createObjectNode();
        wroteDetails.put("endpoint", endpoint);
        wroteDetails.put("key", key);
        if (errorMessage != null) {
            wroteDetails.put("error", errorMessage);
        }
        sometimes(writeSucceeded, "A kv write+read round-trip succeeded at least once", wroteDetails);

        // --- always: an acknowledged write must read back equal ---
        // Only assert the invariant when we actually completed a round-trip.
        // If the attempt threw (node unreachable mid-fault), there is no value
        // to compare and we do not want a false violation, so we skip the
        // always-check on that path.
        if (writeSucceeded) {
            ObjectNode matchDetails = mapper.createObjectNode();
            matchDetails.put("endpoint", endpoint);
            matchDetails.put("key", key);
            matchDetails.put("value", value);
            matchDetails.put("value_matched", valueMatched);
            always(valueMatched,
                "A value written and acknowledged reads back equal on the same node",
                matchDetails);
        }
    }
}

