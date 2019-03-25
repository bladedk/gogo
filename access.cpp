#include "access_ref.h"

namespace breeze {
namespace app {
namespace global_idf {


    ////////////////////////////////////////////////////////////////////////////////
    // GlobalIdfAccessRef

    GlobalIdfAccessRef::GlobalIdfAccessRef(IdfReader* reader)
    {
        m_total_number_of_documents = reader->get_document_count();

        // average doc. length
        const std::map<std::string, length_t>& length_map = reader->get_length_map();
        for (std::map<std::string, length_t>::const_iterator it = length_map.begin();
            it != length_map.end(); ++it)
        {
            m_average_length_map[it->first] = it->second / (double)m_total_number_of_documents;
        }

        std::string term;
        df_type df;
        while (reader->read(term, df))
            m_htable[term] = df;
    }


    df_type GlobalIdfAccessRef::get(const char* term)
    {
        hash_table::iterator it = m_htable.find(term);
        if (it == m_htable.end())
            return 0;
        return it->second;
    }


    void GlobalIdfAccessRef::stats(int& size, int& bucket_count, float& load_factor, float& max_load_factor) const
    {
        size = m_htable.size();
        bucket_count = m_htable.bucket_count();
        load_factor = m_htable.load_factor();
        max_load_factor = m_htable.max_load_factor();
    }


} // global_idf
} // app
} // breeze
