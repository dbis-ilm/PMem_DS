/*
Taken from: https://pmem.io/2017/01/23/cpp-strings.html
*/

#ifndef PString_hpp_
#define PString_hpp_

#include <libpmemobj/tx_base.h>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;

namespace dbis::ptable {

#define SSO_CHARS 15
#define SSO_SIZE (SSO_CHARS + 1)

    class PString {
      public:
      char *data() const { return str ? str.get() : const_cast<char *>(sso); }

      void reset();

      void set(std::string *value);

      private:
      char sso[SSO_SIZE];
      persistent_ptr<char[]> str;
    };

    inline void PString::reset() {
      pmemobj_tx_add_range_direct(sso, 1);
      sso[0] = 0;
      if (str) delete_persistent<char[]>(str, strlen(str.get()) + 1);
    }

    inline void PString::set(std::string *value) {
      unsigned long length = value->length();
      if (length <= SSO_CHARS) {
        if (str) {
          delete_persistent<char[]>(str, strlen(str.get()) + 1);
          str = nullptr;
        }
        pmemobj_tx_add_range_direct(sso, SSO_SIZE);
        strcpy(sso, value->c_str());
      } else {
        if (str) delete_persistent<char[]>(str, strlen(str.get()) + 1);
        str = make_persistent<char[]>(length + 1);
        strcpy(str.get(), value->c_str());
      }
    }
} /* namespace dbis::ptable */

#endif /* PString_hpp_ */