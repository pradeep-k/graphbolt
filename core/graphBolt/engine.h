// Copyright (c) 2020 Mugilan Mariappan, Joanna Che and Keval Vora.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#ifndef KICKSTARTER_ENGINE_H
#define KICKSTARTER_ENGINE_H

#include "bitsetscheduler.h"
#include "parallel.h"
#include "view_interface.h"
#include "bitmap.h"
#include <fstream>
#include <iostream>

using std::ofstream;
using std::ostream;

#define MAX_LEVEL1 65535
#define MAX_PARENT 4294967295

#ifdef EDGEDATA
#else
struct EmptyEdgeData{};
typedef EmptyEdgeData EdgeData;

EdgeData emptyEdgeData;
#endif

// ======================================================================
// VertexValue INITIALIZATION
// ======================================================================
// Set the initial value of the vertex
template <class VertexValueType, class GlobalInfoType>
inline void initializeVertexValue(const vid_t &v,
                                  VertexValueType &v_vertex_value,
                                  const GlobalInfoType &global_info);

// ======================================================================
// ACTIVATE VERTEX FOR FIRST ITERATION
// ======================================================================
// Return whether a vertex should be active when the processing starts. For
// BFS/SSSP, only source vertex returns true. For CC, all vertices return true.
template <class GlobalInfoType>
inline bool frontierVertex(const vid_t &v, const GlobalInfoType &global_info);

// ======================================================================
// EDGE FUNCTION
// ======================================================================
// For an edge (u, v), compute v's value based on u's value.
// Return false if the value from u should not be use to update the value of v.
// Return true otherwise.
template <class VertexValueType, class EdgeDataType, class GlobalInfoType>
inline bool edgeFunction(const vid_t &u, const vid_t &v,
                         const EdgeDataType &edge_data,
                         const VertexValueType &u_value,
                         VertexValueType &v_value, GlobalInfoType &global_info);

// ======================================================================
// SHOULDPROPAGATE
// ======================================================================
// shouldPropagate condition for deciding if the value change in
// updated graph violates monotonicity
template <class VertexValueType, class GlobalInfoType>
inline bool shouldPropagate(const VertexValueType &old_value,
                            const VertexValueType &new_value,
                            GlobalInfoType &global_info);

// ======================================================================
// HELPER FUNCTIONS
// ======================================================================
template <class GlobalInfoType>
void printAdditionalData(ofstream &output_file, const vid_t &v,
                         GlobalInfoType &info);

// ======================================================================
// KICKSTARTER ENGINE
// ======================================================================
template <class T1, class VertexValueType, class GlobalInfoType>
class KickStarterEngine {

public:
  gview_t* my_graph;

  // Current Graph graph
  // number of vertices in current graph
  long n;
  GlobalInfoType &global_info;

  // Previous graph
  // number of vertices in old graph
  long n_old;
  GlobalInfoType global_info_old;

  template <class T> struct DependencyData {
    vid_t parent;
    T value;
    uint16_t level;
    DependencyData() : level(MAX_LEVEL1), value(), parent(MAX_PARENT) {}

    DependencyData(uint16_t _level, T _value, uint32_t _parent)
        : level(_level), value(_value), parent(_parent) {}

    DependencyData(const DependencyData &object)
        : level(object.level), value(object.value), parent(object.parent) {}

    void reset() {
      parent = MAX_PARENT;
      level = MAX_LEVEL1;
    }

    inline bool operator==(const DependencyData &rhs) {
      if ((value == rhs.value) && (parent == rhs.parent) &&
          (level == rhs.level))
        return true;
      else
        return false;
    }

    inline bool operator!=(const DependencyData &rhs) {
      if ((value != rhs.value) || (parent != rhs.parent) ||
          (level != rhs.level))
        return true;
      else
        return false;
    }

    template <class P>
    friend ostream &operator<<(ostream &os, const DependencyData<P> &dt);
  };

  template <class P>
  friend ostream &operator<<(ostream &os, const DependencyData<P> &dt) {
    // os << dt.value << " " << dt.level;
    os << dt.value;
    return os;
  }

  DependencyData<VertexValueType> *dependency_data;
  DependencyData<VertexValueType> *dependency_data_old;

  // TODO : Replace with more efficient vertexSubset using bitmaps
  Bitmap frontier;
  Bitmap changed;
  Bitmap all_affected_vertices;

  BitsetScheduler active_vertices_bitset;

  int current_batch;

  KickStarterEngine(gview_t* _my_graph, GlobalInfoType &_global_info)
      : my_graph(_my_graph), global_info(_global_info), global_info_old(),
        current_batch(0),
        active_vertices_bitset(my_graph->get_vcount()) {
    n = my_graph->get_vcount();
    n_old = 0;
  }

  void init() {
    createDependencyData();
    createTemporaryStructures();
    createVertexSubsets();
    initVertexSubsets();
    initTemporaryStructures();
    initDependencyData();
  }

  ~KickStarterEngine() {
    freeDependencyData();
    freeTemporaryStructures();
    freeVertexSubsets();
    global_info.cleanup();
  }

  // ======================================================================
  // DEPENDENCY DATA STORAGE
  // ======================================================================
  void createDependencyData() {
    dependency_data = newA(DependencyData<VertexValueType>, n);
  }
  void resizeDependencyData() {
    dependency_data =
        renewA(DependencyData<VertexValueType>, dependency_data, n);
    initDependencyData(n_old, n);
  }
  void freeDependencyData() { deleteA(dependency_data); }
  void initDependencyData() { initDependencyData(0, n); }
  void initDependencyData(long start_index, long end_index) {
    parallel_for(long v = start_index; v < end_index; v++) {
      dependency_data[v].reset();
      initializeVertexValue<VertexValueType, GlobalInfoType>(
          v, dependency_data[v].value, global_info);
    }
  }

  // ======================================================================
  // TEMPORARY STRUCTURES USED BY THE ENGINE
  // ======================================================================
  virtual void createTemporaryStructures() {
    dependency_data_old = newA(DependencyData<VertexValueType>, n);
  }
  virtual void resizeTemporaryStructures() {
    dependency_data_old =
        renewA(DependencyData<VertexValueType>, dependency_data_old, n);
    initDependencyData(n_old, n);
  }
  virtual void freeTemporaryStructures() { deleteA(dependency_data_old); }
  virtual void initTemporaryStructures() { initTemporaryStructures(0, n); }
  virtual void initTemporaryStructures(long start_index, long end_index) {}

  // ======================================================================
  // VERTEX SUBSETS USED BY THE ENGINE
  // ======================================================================
  void createVertexSubsets() {
    frontier.init(n);
    changed.init(n);
    all_affected_vertices.init(n);
  }
  void resizeVertexSubsets() {
    //frontier = renewA(bool, frontier, n);
    //all_affected_vertices = renewA(bool, all_affected_vertices, n);
    //changed = renewA(bool, changed, n);
    //initVertexSubsets(n_old, n);
  }
  void freeVertexSubsets() {
  }
  void initVertexSubsets() { initVertexSubsets(0, n); }
  void initVertexSubsets(long start_index, long end_index) {
    /*parallel_for(long j = start_index; j < end_index; j++) {
      frontier[j] = 0;
      changed[j] = 0;
    }*/
  }

  void processVertexAddition(long maxVertex) {
    n_old = n;
    n = maxVertex + 1;
    resizeDependencyData();
    resizeTemporaryStructures();
    resizeVertexSubsets();
  }

  void printOutput() {
    int level = 0;
    int vid_count = 0;
    do {
        vid_count = 0;
        for (vid_t v = 0; v < n; v++) {
            if (dependency_data[v].level == level) {
                ++vid_count;
                if (level == 0) cout << "root = " << v << endl;
            }
            
        }
        cout << level << ":" << vid_count << endl;
        ++level;
    } while (vid_count);
    
    /*****/
    cout << "\n";
    current_batch++;
  }

  void initialCompute() {
    active_vertices_bitset.reset();

    parallel_for(vid_t v = 0; v < n; v++) {
      if (frontierVertex(v, global_info)) {
        active_vertices_bitset.schedule(v);
        dependency_data[v].level = 0;
        dependency_data[v].parent = v;
      }
    }

    traditionalIncrementalComputation();
    //printOutput();
  }

  // TODO : Write a lock based reduce function. Add functionality to use the
  // lock based reduce function depending on the size of DependendencyData              
  bool reduce(const vid_t &u, const vid_t &v, const EdgeData &edge_data,
              const DependencyData<VertexValueType> &u_data,
              DependencyData<VertexValueType> &v_data, GlobalInfoType &info) {
    DependencyData<VertexValueType> newV, oldV;
    DependencyData<VertexValueType> incoming_value_curr = u_data;

    bool ret = edgeFunction(u, v, edge_data, incoming_value_curr.value, newV.value, info);
    if (!ret) {
      return false;
    }
    newV.level = incoming_value_curr.level + 1;
    newV.parent = u;

    bool update_successful = true;
    do {
      oldV = v_data;
      // If oldV is lesser than the newV computed frm u, we should update.
      // Otherwise, break
      if ((shouldPropagate(oldV.value, newV.value, global_info)) ||
          ((oldV.value == newV.value) && (oldV.level <= newV.level))) {
        update_successful = false;
        break;
      }
    } while (!CAS(&v_data, oldV, newV));
    return update_successful;
  }
  
  bool reduce_noatomic(const vid_t &u, const vid_t &v, const EdgeData &edge_data,
              const DependencyData<VertexValueType> &u_data,
              DependencyData<VertexValueType> &v_data, GlobalInfoType &info) {
    DependencyData<VertexValueType> newV, oldV;
    DependencyData<VertexValueType> incoming_value_curr = u_data;

    bool ret = edgeFunction(u, v, edge_data, incoming_value_curr.value, newV.value, info);
    if (!ret) {
      return false;
    }
    newV.level = incoming_value_curr.level + 1;
    newV.parent = u;

    bool update_successful = true;
      oldV = v_data;
      // If oldV is lesser than the newV computed frm u, we should update.
      // Otherwise, break
      if ((shouldPropagate(oldV.value, newV.value, global_info)) ||
          ((oldV.value == newV.value) && (oldV.level <= newV.level))) {
        return  false;
      }
      v_data = newV;
    return update_successful;
  }

  int traditionalIncrementalComputation() {
    while (active_vertices_bitset.anyScheduledTasks()) {
      active_vertices_bitset.newIteration();

      parallel_for(vid_t u = 0; u < n; u++) {
        if (active_vertices_bitset.isScheduled(u)) {
          // process all its outNghs
          nebr_reader_t adj_list;
          T1* dst;
          intE outDegree = my_graph->get_nebrs_out(u, adj_list) ;//V[u].getOutDegree();
          granular_for(i, 0, outDegree, (outDegree > 1024), {
            dst = adj_list.get_item<T1>(i);
            vid_t v = TO_VID(get_sid(*dst));
#ifdef EDGEDATA
            EdgeData *edge_data = my_graph->V[u].getOutEdgeData(i);
#else
            EdgeData *edge_data = &emptyEdgeData;
#endif
            bool ret = reduce(u, v, *edge_data, dependency_data[u], dependency_data[v],
                              global_info);
            if (ret) {
              active_vertices_bitset.schedule(v);
            }
          });
        }
      }
    }
  }

  void bfs_2ddir() {
    edge_t*  tmp = 0;
    index_t edge_count = my_graph->get_new_edges1(tmp);
    edgeT_t<T1>* edges = (edgeT_t<T1>*)tmp;

    parallel_for(long i = 0; i < edge_count; i++) {
      vid_t  source     = TO_VID(get_src(edges[i]));
      vid_t destination =  TO_VID(get_dst(edges+i));
      bool is_del = IS_DEL(get_src(edges[i]));
      if (is_del) {
          if (dependency_data[destination].parent == source) {
            dependency_data[destination].reset();
            initializeVertexValue<VertexValueType, GlobalInfoType>(
                destination, dependency_data[destination].value, global_info);
            active_vertices_bitset.schedule(destination);
            all_affected_vertices.set_bit_atomic(destination);
          }
      }
    }
  }
  
  void bfs_2udir() {
    edge_t*  tmp = 0;
    index_t edge_count = my_graph->get_new_edges1(tmp);
    edgeT_t<T1>* edges = (edgeT_t<T1>*)tmp;

    parallel_for(long i = 0; i < edge_count; i++) {
      vid_t  source     = TO_VID(get_src(edges[i]));
      vid_t destination =  TO_VID(get_dst(edges+i));
      bool is_del = IS_DEL(get_src(edges[i]));
      if (is_del) {
          if (dependency_data[destination].parent == source) {
            dependency_data[destination].reset();
            initializeVertexValue<VertexValueType, GlobalInfoType>(
                destination, dependency_data[destination].value, global_info);
            active_vertices_bitset.schedule(destination);
            all_affected_vertices.set_bit_atomic(destination);
          } else if (dependency_data[source].parent == destination) {
            dependency_data[source].reset();
            initializeVertexValue<VertexValueType, GlobalInfoType>(
                source, dependency_data[source].value, global_info);
            active_vertices_bitset.schedule(source);
            all_affected_vertices.set_bit_atomic(source);
          }
      }
    }
  }
  
  void bfs_4ddir() {
    edge_t*  tmp = 0;
    index_t edge_count = my_graph->get_new_edges1(tmp);
    edgeT_t<T1>* edges = (edgeT_t<T1>*)tmp;

    parallel_for(long i = 0; i < edge_count; i++) {
      vid_t  source     = TO_VID(get_src(edges[i]));
      vid_t destination =  TO_VID(get_dst(edges+i));
      bool is_del = IS_DEL(get_src(edges[i]));
#ifdef EDGEDATA
      EdgeData *edge_data = edge_additions.E[i].edgeData;
#else
      EdgeData *edge_data = &emptyEdgeData;
#endif
      if (false == is_del) {
          bool ret = reduce(source, destination, *edge_data, dependency_data[source],
                            dependency_data[destination], global_info);
          if (ret) {
            all_affected_vertices.set_bit_atomic(destination);
          }
      }
    }
  }
  
  void bfs_4udir() {
    edge_t*  tmp = 0;
    index_t edge_count = my_graph->get_new_edges1(tmp);
    edgeT_t<T1>* edges = (edgeT_t<T1>*)tmp;

    parallel_for(long i = 0; i < edge_count; i++) {
      vid_t  source     = TO_VID(get_src(edges[i]));
      vid_t destination =  TO_VID(get_dst(edges+i));
      bool is_del = IS_DEL(get_src(edges[i]));
#ifdef EDGEDATA
      EdgeData *edge_data = edge_additions.E[i].edgeData;
#else
      EdgeData *edge_data = &emptyEdgeData;
#endif
      if (false == is_del) {
          bool ret = reduce(source, destination, *edge_data, dependency_data[source],
                            dependency_data[destination], global_info);
          if (ret) {
            all_affected_vertices.set_bit_atomic(destination);
          }
          ret = reduce(destination, source, *edge_data, dependency_data[destination],
                            dependency_data[source], global_info);
          if (ret) {
            all_affected_vertices.set_bit_atomic(source);
          }
      }
    }
  }

  void deltaCompute() {
    // Handle newly added vertices
    n_old = n;

    // Reset values before incremental computation
    active_vertices_bitset.reset();
    // all_affected_vertices is used only for switching purposes
    all_affected_vertices.reset();
    frontier.reset();
    changed.reset();
    parallel_for(vid_t v = 0; v < n; v++) {
      // Make a copy of the old dependency data
      dependency_data_old[v] = dependency_data[v];
    }
    // ======================================================================
    // PHASE 1 - Update global_info
    // ======================================================================
    // Pretty much nothing is going to happen here. But, maintaining consistency with GraphBolt
    global_info_old.copy(global_info);

    // ======================================================================
    // PHASE 2 = Identify vertex values affected by edge deletions
    // ======================================================================
    if (my_graph->is_ddir()) {
        bfs_2ddir();
    } else {
        bfs_2udir();
    }

    // ======================================================================
    // PHASE 3 - Trimming phase
    // ======================================================================
    while (active_vertices_bitset.anyScheduledTasks()) {

      // For all the vertices 'v' affected, update value of 'v' from its
      // inNghs, such that level(v) > level(inNgh) in the old dependency tree
      active_vertices_bitset.newIteration();

      parallel_for(vid_t v = 0; v < n; v++) {
        if (active_vertices_bitset.isScheduled(v)) {
          nebr_reader_t adj_list;
          T1* dst;
          intE inDegree = my_graph->get_nebrs_in(v, adj_list);
          DependencyData<VertexValueType> v_value_old = dependency_data[v];

          parallel_for(intE i = 0; i < inDegree; i++) {
            dst = adj_list.get_item<T1>(i);
            vid_t u = TO_VID(get_sid(*dst));
            // Process inEdges with smallerLevel than currentVertex.
            if (dependency_data_old[v].level > dependency_data_old[u].level) {
#ifdef EDGEDATA
              EdgeData *edge_data = my_graph->V[v].getInEdgeData(i);
#else
              EdgeData *edge_data = &emptyEdgeData;
#endif
              bool ret =
                  reduce_noatomic(u, v, *edge_data, dependency_data[u], v_value_old, global_info);
            }
          }
          // Evaluate the shouldReduce condition.. See if the new value is
          // greater than the old value
          if ((shouldPropagate(dependency_data_old[v].value,
                               dependency_data[v].value, global_info)) ||
              (shouldPropagate(v_value_old.value, dependency_data[v].value,
                               global_info))) {
            changed.set_bit(v);
          }
        }
      }

      parallel_for(vid_t v = 0; v < n; v++) {
        if (changed.get_bit(v)) {
          changed.reset_bit(v);
          // Push down in dependency tree
          nebr_reader_t adj_list;
          T1* dst;
          intE outDegree = my_graph->get_nebrs_out(v, adj_list) ;//V[u].getOutDegree();
          DependencyData<VertexValueType> v_value = dependency_data[v];
          
          parallel_for(intE i = 0; i < outDegree; i++) {
            dst = adj_list.get_item<T1>(i);
            vid_t w = TO_VID(get_sid(*dst));
            // Push the changes down only to its outNghs in the dependency
            // tree
            if (dependency_data[w].parent == v) {
              DependencyData<VertexValueType> newV, oldV;
              oldV = dependency_data[w];

              // Reset dependency_data[w]
              dependency_data[w].reset();
              initializeVertexValue<VertexValueType, GlobalInfoType>(
                  w, dependency_data[w].value, global_info);
              newV = dependency_data[w];

              // Update w's value based on u's value if needed
#ifdef EDGEDATA
              EdgeData *edge_data = my_graph->V[v].getOutEdgeData(i);
#else
              EdgeData *edge_data = &emptyEdgeData;
#endif
              bool ret = reduce(v, w, *edge_data, v_value, newV, global_info);

              if ((oldV.value != newV.value) || (oldV.level != newV.level)) {
                dependency_data[w] = newV;
                all_affected_vertices.set_bit_atomic(w);

                if ((shouldPropagate(dependency_data_old[w].value, newV.value,
                                     global_info)) ||
                    (shouldPropagate(oldV.value, newV.value, global_info))) {
                  active_vertices_bitset.schedule(w);
                }
                if (shouldPropagate(oldV.value, newV.value, global_info)) {
                  frontier.set_bit_atomic(w);
                }
              }
            }
          }
        }
      }
      changed.swap(&frontier);
    }
    //===========================================
    //==========================================

    // Pull once for all the affected vertices
    parallel_for(vid_t v = 0; v < n; v++) {
      if (all_affected_vertices.get_bit(v) == true) {
        nebr_reader_t adj_list;
        T1* dst;
        intE inDegree = my_graph->get_nebrs_in(v, adj_list);
        parallel_for(intE i = 0; i < inDegree; i++) {
            dst = adj_list.get_item<T1>(i);
            vid_t u = TO_VID(get_sid(*dst));
#ifdef EDGEDATA
          EdgeData *edge_data = my_graph->V[v].getInEdgeData(i);
#else
          EdgeData *edge_data = &emptyEdgeData;
#endif
          bool ret =
              reduce_noatomic(u, v, *edge_data, dependency_data[u], dependency_data[v], global_info);
        }
      }
    }

    // ======================================================================
    // PHASE 4 - Process additions
    // ======================================================================
    if (my_graph->is_ddir()) {
        bfs_4ddir();
    } else {
        bfs_4udir();
    }

    // ======================================================================
    // PHASE 5 - Traditional processing
    // ======================================================================
    // For all affected vertices, start traditional processing
    active_vertices_bitset.reset();
    parallel_for(vid_t v = 0; v < n; v++) {
      if (all_affected_vertices.get_bit(v) == true) {
        active_vertices_bitset.schedule(v);
      }
    }
    traditionalIncrementalComputation();

    //printOutput();
  }
};

#endif
