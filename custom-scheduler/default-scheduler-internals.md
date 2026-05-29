# Kubernetes Default Scheduler: Under the Hood

The Kubernetes default scheduler (`kube-scheduler`) is a core control plane component responsible for assigning newly created Pods to appropriate Nodes in the cluster. It constantly watches the API server for unscheduled Pods (Pods where `spec.nodeName` is empty) and finds the best Node for them based on constraints and available resources.

This document breaks down the scheduling process, focusing on the modern **Scheduling Framework** architecture that powers it.

---

## 1. High-Level Architecture: The Two Cycles

The scheduling process for a single Pod is divided into two distinct cycles:

1. **Scheduling Cycle:** Selects a Node for the Pod. This cycle is executed **sequentially**. Only one Pod is processed at a time to avoid race conditions and ensure accurate resource accounting.
2. **Binding Cycle:** Applies the decision to the cluster. This cycle is executed **concurrently**. The scheduler tells the API server to assign the Pod to the chosen Node.

If a Pod is determined to be unschedulable (e.g., no Node has enough resources), it is returned to the scheduling queue, and the scheduler moves on to the next Pod.

---

## 2. The Scheduling Framework

The `kube-scheduler` is built on the **Scheduling Framework**, a set of pluggable extension points. By default, Kubernetes compiles standard behaviors into these extension points, but custom schedulers can add or replace them.

The journey of a Pod through the scheduler involves passing through these extension points in a specific order:

### A. Queue Sort (Pre-scheduling)
Before scheduling even begins, Pods sit in a scheduling queue. 
* **Under the hood:** The default scheduler uses a priority queue. Pods are sorted by their PriorityClass. Higher priority Pods are placed at the front of the queue and evaluated first.

### B. Pre-Filter
* **Goal:** Pre-processing and validation.
* **Action:** Checks if the Pod is eligible for scheduling and computes any state needed by later plugins. For instance, it might check if a Pod has specified a node selector that matches zero nodes, failing early.

### C. Filter (Formerly "Predicates")
* **Goal:** Eliminate Nodes that *cannot* run the Pod.
* **Action:** The scheduler evaluates every Node against a series of "Filter" plugins. If a Node fails *any* filter, it is immediately discarded for this Pod.
* **Common Default Filters:**
    * `PodFitsResources`: Does the Node have enough CPU/Memory/Ephemeral Storage for the Pod's requests?
    * `NodePorts`: Does the Pod request a specific host port that is already in use on this Node?
    * `NodeAffinity`: Does the Node match the Pod's `nodeAffinity` or `nodeSelector`?
    * `TaintToleration`: Does the Node have taints that the Pod does not tolerate?
    * `VolumeBinding`: Can the requested PersistentVolumeClaims be bound to this Node?

### D. Post-Filter
* **Goal:** Handle unschedulable Pods.
* **Action:** If the Filter phase eliminates *all* Nodes, the Pod is unschedulable. The Post-Filter phase is triggered. Its main job in the default scheduler is **Preemption**. It tries to find lower-priority Pods that can be evicted to make room for this pending, high-priority Pod.

### E. Pre-Score
* **Goal:** Prepare state for the Scoring phase.
* **Action:** Similar to Pre-Filter, this is an optimization step to calculate shared data (like gathering a list of all Pods on a Node) so that individual Score plugins don't have to duplicate the work.

### F. Score (Formerly "Priorities")
* **Goal:** Rank the remaining eligible Nodes to find the *best* one.
* **Action:** The scheduler runs the remaining Nodes through a series of "Score" plugins. Each plugin gives each Node a score between 0 and 100.
* **Common Default Score Plugins:**
    * `NodeResourcesBalancedAllocation`: Favors Nodes where CPU and Memory usage fractions will be balanced after placing the Pod.
    * `NodeResourcesFit`: Favors Nodes that have more unallocated resources (or fewer, depending on configuration—typically spreads workloads out).
    * `InterPodAffinity`: Scores higher if placing the Pod satisfies preferred inter-pod affinities.
    * `TaintToleration`: Penalizes Nodes that have "preferNoSchedule" taints.
    * `ImageLocality`: Favors Nodes that already have the required container images downloaded.

### G. Normalize Score
* **Goal:** Combine all scores into a final ranking.
* **Action:** Each Score plugin is assigned a weight. The scheduler calculates a final score for each Node: `FinalScore = sum(PluginScore * PluginWeight)`. 
* **The Decision:** The Node with the highest FinalScore is selected. If multiple Nodes tie for the highest score, the scheduler picks one randomly using a round-robin algorithm.

---

## 3. The Binding Cycle

Once a Node is selected, the Scheduling Cycle ends, and the Binding Cycle begins (concurrently).

### A. Reserve
* **Goal:** Avoid race conditions.
* **Action:** The scheduler updates its internal cache to "reserve" the resources on the selected Node for this Pod. This prevents the scheduler from accidentally assigning those same resources to the next Pod in the queue while the actual binding operation (which involves network calls) is happening.

### B. Permit
* **Goal:** Optional delay for external approvals.
* **Action:** Plugins can pause the binding process here. This is rarely used in the default setup but is useful for custom gang-scheduling (waiting until a whole group of Pods is ready to be scheduled together).

### C. Pre-Bind
* **Goal:** Prepare the cluster for the Pod.
* **Action:** Often used to provision necessary infrastructure. The most common default action here is provisioning network volumes (like AWS EBS or GCE PD) so they are ready before the Pod starts running.

### D. Bind
* **Goal:** Tell the API Server the decision.
* **Action:** The scheduler creates a `Binding` object in the Kubernetes API. This updates the Pod's `spec.nodeName` to the chosen Node.

### E. Post-Bind
* **Goal:** Informational cleanup.
* **Action:** The binding is complete. This is used for logging or updating internal metrics.

---

## 4. Under the Hood: Event-Driven Architecture

The `kube-scheduler` doesn't constantly poll the API server. It uses an efficient, event-driven model:

1. **Informers:** The scheduler uses client-go `SharedInformer`s to watch the API Server for changes to Pods, Nodes, PersistentVolumes, etc.
2. **Local Cache:** It maintains a highly optimized, in-memory cache of the cluster state (what Nodes exist, what Pods are on them, what resources are available). This cache is updated instantly via Informer events.
3. **Scheduling Queue:** When an Informer detects a new Pod with an empty `nodeName`, it adds it to the ActiveQ (Active Queue). 
4. **Backoff and Retry:** 
    * If a Pod is unschedulable, it moves to an UnschedulableQ.
    * When cluster state changes (e.g., a Node is added, or a Pod is deleted), Pods in the UnschedulableQ are moved back to the ActiveQ or BackoffQ to be retried.

By evaluating the cluster state from its local cache rather than querying the API server for every decision, the default scheduler achieves high throughput and low latency.
