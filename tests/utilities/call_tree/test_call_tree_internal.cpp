#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <dftracer/utils/call_tree/internal/call_tree.h>
#include <dftracer/utils/call_tree/internal/factory.h>
#include <dftracer/utils/call_tree/internal/node.h>
#include <dftracer/utils/call_tree/internal/process_call_tree.h>
#include <dftracer/utils/call_tree/internal/process_key.h>
#include <doctest/doctest.h>

#include <memory>
#include <string>

using namespace dftracer::utils::call_tree::internal;

TEST_CASE("CallTree - Basic construction and initialization") {
    CallTree tree;

    SUBCASE("Initialize tree") {
        tree.initialize();
        // Tree is initialized - verify we can use it
        CHECK(tree.get(1234, 5678) != nullptr);
    }

    SUBCASE("Cleanup after initialize") {
        tree.initialize();
        tree.cleanup();
        // After cleanup, tree should still be usable but empty
    }
}

TEST_CASE("ProcessKey - Construction and comparison") {
    SUBCASE("Default construction") {
        ProcessKey key1;
        ProcessKey key2;
        CHECK(key1 == key2);
    }

    SUBCASE("Construction with pid and tid") {
        ProcessKey key1(1234, 5678);
        CHECK(key1.pid == 1234);
        CHECK(key1.tid == 5678);
    }

    SUBCASE("Comparison operators") {
        ProcessKey key1(100, 200);
        ProcessKey key2(100, 200);
        ProcessKey key3(100, 201);
        ProcessKey key4(101, 200);

        CHECK(key1 == key2);
        CHECK_FALSE(key1 == key3);
        CHECK_FALSE(key1 == key4);
        CHECK(key1 != key3);
        CHECK(key1 != key4);
    }

    SUBCASE("Hash consistency") {
        ProcessKey key1(100, 200, 300);
        ProcessKey key2(100, 200, 300);
        ProcessKey key3(100, 201, 300);

        // Same keys should hash the same
        std::hash<ProcessKey> hasher;
        CHECK(hasher(key1) == hasher(key2));
        // Different keys may hash differently (not guaranteed but likely)
        CHECK(hasher(key1) != hasher(key3));
    }
}

TEST_CASE("CallTreeFactory - Create nodes") {
    CallTreeFactory factory;
    factory.initialize();

    SUBCASE("Create simple node") {
        auto node =
            factory.create_node(1, "test_function", "function", 1000, 500, 0);

        CHECK(node != nullptr);
        CHECK(node->get_id() == 1);
        CHECK(node->get_name() == "test_function");
        CHECK(node->get_category() == "function");
        CHECK(node->get_start_time() == 1000);
        CHECK(node->get_duration() == 500);
        CHECK(node->get_level() == 0);
    }

    SUBCASE("Create node with arguments") {
        std::unordered_map<std::string, std::string> args;
        args["arg1"] = "value1";
        args["arg2"] = "value2";

        auto node = factory.create_node(2, "test_func", "category", 2000, 1000,
                                        1, args);

        CHECK(node != nullptr);
        CHECK(node->get_args().size() == 2);
        CHECK(node->get_args().at("arg1") == "value1");
        CHECK(node->get_args().at("arg2") == "value2");
    }

    SUBCASE("Multiple nodes with unique IDs") {
        auto node1 = factory.create_node(1, "func1", "cat1", 100, 50, 0);
        auto node2 = factory.create_node(2, "func2", "cat2", 200, 60, 1);

        CHECK(node1->get_id() != node2->get_id());
        CHECK(factory.get_node_count() == 2);
    }

    factory.cleanup();
}

TEST_CASE("CallNode - Parent-child relationships") {
    CallTreeFactory factory;
    factory.initialize();

    SUBCASE("Set parent ID") {
        auto node =
            factory.create_node(1, "child_func", "function", 1000, 500, 1);

        node->set_parent_id(100);
        CHECK(node->get_parent_id() == 100);
    }

    SUBCASE("Add children") {
        auto parent =
            factory.create_node(1, "parent_func", "function", 1000, 1000, 0);

        parent->add_child(2);
        parent->add_child(3);
        parent->add_child(4);

        const auto& children = parent->get_children();
        CHECK(children.size() == 3);
        CHECK(children[0] == 2);
        CHECK(children[1] == 3);
        CHECK(children[2] == 4);
    }

    factory.cleanup();
}

TEST_CASE("ProcessCallTree - Basic operations") {
    ProcessKey key(1234, 5678);
    ProcessCallTree graph;
    graph.key = key;

    SUBCASE("Process key is set correctly") {
        CHECK(graph.key.pid == 1234);
        CHECK(graph.key.tid == 5678);
    }

    SUBCASE("Start with empty root calls") { CHECK(graph.root_calls.empty()); }

    SUBCASE("Start with empty call map") { CHECK(graph.calls.empty()); }

    SUBCASE("Add root call") {
        graph.root_calls.push_back(1);
        CHECK(graph.root_calls.size() == 1);
        CHECK(graph.root_calls[0] == 1);
    }

    SUBCASE("Add call sequence") {
        graph.call_sequence.push_back(1);
        graph.call_sequence.push_back(2);
        graph.call_sequence.push_back(3);

        CHECK(graph.call_sequence.size() == 3);
    }
}

TEST_CASE("CallTree - Process graph operations") {
    CallTree tree;
    tree.initialize();

    SUBCASE("Access process graph creates it if not exists") {
        ProcessKey key(1000, 2000);
        auto& graph = tree[key];

        CHECK(graph.key == key);
    }

    SUBCASE("Multiple process graphs") {
        ProcessKey key1(1000, 2000);
        ProcessKey key2(1000, 3000);
        ProcessKey key3(2000, 2000);

        auto& graph1 = tree[key1];
        auto& graph2 = tree[key2];
        auto& graph3 = tree[key3];

        CHECK(graph1.key == key1);
        CHECK(graph2.key == key2);
        CHECK(graph3.key == key3);
    }

    SUBCASE("Get factory") {
        auto& factory = tree.get_factory();
        auto node = factory.create_node(1, "test", "cat", 100, 50, 0);
        CHECK(node != nullptr);
    }

    tree.cleanup();
}

TEST_CASE("CallTree - Integration test with nodes") {
    CallTree tree;
    tree.initialize();

    ProcessKey key(1234, 5678);
    auto& graph = tree[key];
    auto& factory = tree.get_factory();

    SUBCASE("Build simple call tree") {
        // Create root node
        auto root = factory.create_node(1, "main", "function", 0, 1000, 0);

        // Create child nodes
        auto child1 = factory.create_node(2, "func1", "function", 100, 200, 1);
        auto child2 = factory.create_node(3, "func2", "function", 400, 300, 1);

        // Set up relationships
        child1->set_parent_id(1);
        child2->set_parent_id(1);
        root->add_child(2);
        root->add_child(3);

        // Add to graph
        graph.calls[1] = root;
        graph.calls[2] = child1;
        graph.calls[3] = child2;
        graph.root_calls.push_back(1);
        graph.call_sequence = {1, 2, 3};

        // Verify structure
        CHECK(graph.root_calls.size() == 1);
        CHECK(graph.calls.size() == 3);
        CHECK(graph.call_sequence.size() == 3);
        CHECK(root->get_children().size() == 2);
    }

    tree.cleanup();
}
