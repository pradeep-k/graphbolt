#pragma once

// ======================================================================
// BFSINFO
// ======================================================================
class BfsInfo {
public:
  vid_t source_vertex;

  BfsInfo() : source_vertex(0) {}

  BfsInfo(vid_t _source_vertex) : source_vertex(_source_vertex) {}

  void copy(const BfsInfo &object) { source_vertex = object.source_vertex; }

  void cleanup() {}

  void init() {}
};

// ======================================================================
// VERTEXVALUE INITIALIZATION
// ======================================================================
template <class VertexValueType, class GlobalInfoType>
inline void initializeVertexValue(const vid_t &v,
                                  VertexValueType &v_vertex_value,
                                  const GlobalInfoType &global_info) {
  if (v != global_info.source_vertex) {
    v_vertex_value = 0;
  } else {
    v_vertex_value = 1;
  }
}

// ======================================================================
// ACTIVATE VERTEX FOR FIRST ITERATION
// ======================================================================
template <class GlobalInfoType>
inline bool frontierVertex(const vid_t &v, const GlobalInfoType &global_info) {
  if (v == global_info.source_vertex) {
    return true;
  } else {
    return false;
  }
}

// ======================================================================
// EDGE FUNCTION
// ======================================================================
template <class VertexValueType, class EdgeDataType, class GlobalInfoType>
inline bool
edgeFunction(const vid_t &u, const vid_t &v, const EdgeDataType &edge_weight,
             const VertexValueType &u_value, VertexValueType &v_value,
             GlobalInfoType &global_info) {
  if (u_value == 0) {
    return false;
  } else {
    v_value = 1;
    return true;
  }
}

// ======================================================================
// SHOULDPROPAGATE
// ======================================================================
// shouldPropagate condition for deciding if the value change in
// updated graph violates monotonicity
template <class VertexValueType, class GlobalInfoType>
inline bool shouldPropagate(const VertexValueType &old_value,
                            const VertexValueType &new_value,
                            GlobalInfoType &global_info) {
  return (old_value == 1) && (new_value == 0);
}

// ======================================================================
// HELPER FUNCTIONS
// ======================================================================
template <class GlobalInfoType>
void printAdditionalData(ofstream &output_file, const vid_t &v,
                         GlobalInfoType &info) {}

