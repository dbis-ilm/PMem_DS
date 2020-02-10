#include "catch.hpp"
#include "config.h"
#define UNIT_TESTS 1
#include "woPSkiplist.hpp"

using pmem::obj::delete_persistent_atomic;
using pmem::obj::pool;

TEST_CASE("Insert and lookup key") {
	using woPSkip = woPSkiplist<int, int, 8, 8>;

	struct root {
		pptr<woPSkip> skiplist;
	};

	pool<root> pop;
	const std::string path = dbis::gPmemPath + "woPSkiplistTest";

    //std::remove(path.c_str());
    if (access(path.c_str(), F_OK) != 0)
      pop = pool<root>::create(path, "woPSkiplist", ((size_t)(1024 * 1024 * 16)));
    else
      pop = pool<root>::open(path, "woPSkiplist");

	auto q = pop.root();
	auto &rootRef = *q;

	const auto alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.128.desc",
                                                                     PBPTreeType4::AllocClass);

	if(!rootRef.skiplist)
		transaction::run(pop, [&] {rootRef.skiplist = make_persistent<woPSkip>(alloc_class); });

	SECTION("Inserting keys") {
		auto &sl = *rootRef.skiplist;
		for(int i=0; i<10; ++i) {
			sl.insert(i, i*i);
		}

		REQUIRE(sl.search(5) != nullptr);
	
	}

}
