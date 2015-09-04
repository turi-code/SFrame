
#include "DatoCore.hpp"

// [[Rcpp::export]]
SEXP weighted_pagerank(SEXP sg, SEXP num_iters, SEXP field) {
    graphlab::gl_sgraph g = Rcpp::as<graphlab::gl_sgraph>(sg);
    int iters = Rcpp::as<int>(num_iters);
    std::string weight_field = Rcpp::as<std::string>(field);
    const double RESET_PROB=0.15;

    // Get rid of unused fields.
    graphlab::gl_sgraph g_min = g.select_fields({weight_field});

    // Count the total out going weight of each vertex into an SFrame.
    graphlab::gl_sframe outgoing_weight = g_min.get_edges().groupby({"__src_id"},
    {{"total_weight", graphlab::aggregate::SUM(weight_field)}});

    // Add the total_weight to the graph as vertex atrribute.
    // We can update the vertex data by adding the same vertex.
    graphlab::gl_sgraph g2 = g_min.add_vertices(outgoing_weight, "__src_id");

    // Lambda function for triple_apply
    auto pr_update = [&weight_field](graphlab::edge_triple& triple)->void {
        double weight = triple.edge[weight_field];
        triple.target["pagerank"] += triple.source["pagerank_prev"] * weight / triple.source["total_weight"];
    };

    // Initialize pagerank value
    g2.vertices()["pagerank_prev"] = 1.0;

    // Iteratively run triple_apply with the update function
    for (size_t i = 0; i < iters; ++i) {

        g2.vertices()["pagerank"] = 0.0;

        Rcpp::Rcout << "Iteration " << (i+1) << std::endl;
        g2 = g2.triple_apply(pr_update, {"pagerank"});

        g2.vertices()["pagerank"] = RESET_PROB + (1-RESET_PROB) * g2.vertices()["pagerank"];
        g2.vertices()["pagerank_prev"] = g2.vertices()["pagerank"];
    }

    g2.vertices().remove_column("pagerank_prev");
    g2.vertices().remove_column("total_weight");
    return Rcpp::wrap(g2);
}
