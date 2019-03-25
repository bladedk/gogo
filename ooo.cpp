
자신의 로컬 컴퓨터에서 코드를 추가하는 작업은 branch를 만들어서 진행한다.
개발을 하다 보면 코드를 여러 개로 복사해야 하는 일이 자주 생긴다. 코드를 통째로 복사하고 나서 원래 코드와는 상관없이 독립적으로 개발을 진행할 수 있는데, 이렇게 독립적으로 개발하는 것이 브랜치다. 


- pro git book
 // GlobalIdfAccessRef
 //
 //     GlobalIdfAccessRef::GlobalIdfAccessRef(IdfReader* reader)
 //         {
 //                 m_total_number_of_documents = reader->get_document_count();
 //
 //                         // average doc. length
 //                                 const std::map<std::string, length_t>& length_map = reader->get_length_map();
 //                                         for (std::map<std::string, length_t>::const_iterator it = length_map.begin();
 //                                                     it != length_map.end(); ++it)
 //                                                             {
 //                                                                         m_average_length_map[it->first] = it->second / (double)m_total_number_of_documents;
 //
