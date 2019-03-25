include <iostream>
#include <map>

#include "utils/common/logger_config.h"
#include "utils/common/option_parser.h"
#include "utils/common/filesystem.h"
#include "utils/common/json_writer.h"

#include "utils/config/config_impl.h"
#include "utils/cdl/controller_impl.h"
#include "utils/rpc_ice/rpc_ice_impl.h"

#include "app/status/status_common.h"
#include "app/lock_service/sessionless_client.h"
#include "app/lock_service/sessionless_client_impl.h"

#include "app/bridge/client.h"

#include "app/slice/bridge.h"

#define TEMP_ADDRS "brzaddr.tmp"

using namespace breeze;

class ClientWrapper
{
public :
    ClientWrapper(const std::string &config_path)
    {
        // make com
        std::vector<std::string> str_list;
        str_list.push_back(utils::format("--Ice.Config=%s/conf/ice_config", PACKAGE_PREFIX));
        m_com.reset(new rpc::CommunicatorGuardImpl(str_list));

        m_status_object_factory_builder.reset(new breeze::status::StatusObjectFactoryBuilder(m_com.get()));

        // make object_adaptor
        m_object_adaptor.reset(new rpc::ObjectAdaptorImpl(m_com.get(), "tcp"));
        m_object_adaptor->activate();

        // make lock_service_client
        m_config.reset(new config::BreezeConfigImpl(config_path, config_path));
        m_cdl_controller.reset(new cdl::CdlControllerImpl(m_config.get()));

        m_lock_service_client.reset(new app::lock_service::SessionlessClientImpl(m_com.get(), m_config.get(), m_cdl_controller.get()));

        // make bridge_client
        m_bridge_client.reset(new app::bridge::Client(m_com.get(), m_cdl_controller.get(), m_lock_service_client.get()));

        // make ice_run thread
        pthread_create(&m_thread_id, NULL, ClientWrapper::ice_run_thread, (void *)this);
    }

    ~ClientWrapper()
    {
        // ice stop
        m_com->shutdown();
        m_object_adaptor->deactivate();
    }

    app::bridge::Client *get()
    {
        return m_bridge_client.get();
    }

    void ice_run()
    {
        m_com->wait_for_shutdown();
    }

    static void *ice_run_thread(void *arg)
    {
        ClientWrapper *client_wrapper = (ClientWrapper *)arg;

        client_wrapper->ice_run();

        return NULL;
    }

private :
    std::auto_ptr<rpc::CommunicatorGuard> m_com;
    std::auto_ptr<rpc::ObjectAdaptor> m_object_adaptor;
    std::auto_ptr<breeze::status::StatusObjectFactoryBuilder> m_status_object_factory_builder;
    std::auto_ptr<config::BreezeConfig> m_config;
    std::auto_ptr<cdl::CdlController> m_cdl_controller;
    std::auto_ptr<app::lock_service::SessionlessClient> m_lock_service_client;
    std::auto_ptr<app::bridge::Client> m_bridge_client;
    pthread_t m_thread_id;
};


int to_integer(const std::string& strtype)
{
    if(strtype.find_first_not_of("0123456789") != std::string::npos)
        throw utils::CException(utils::format("can't convert to integer %s", strtype.c_str()));

    return ::atoi(strtype.c_str());
}

std::string get_file_content(const std::string& path)
{
    std::string content;
    if(path.size() != 0 and ::access(path.c_str(), R_OK) == 0)
        content = utils::get_file_value(path);
    else
        throw utils::CException(breeze::utils::format("can't read file %s", path.c_str()));

    return content;
}

breeze::app::bridge::IndexingCommand::Type indexing_type(const std::string& cmd)
{
    int type;
    if(cmd == "add_document")  {
        type = app::bridge::IndexingCommand::ADD;
    } else if(cmd == "update")  {
        type = app::bridge::IndexingCommand::UPDATE;
    } else if(cmd == "conditional_update")  {
        type = app::bridge::IndexingCommand::CONDITIONAL_UPDATE;
    } else if(cmd == "delete")  {
        type = app::bridge::IndexingCommand::DELETE;
    } else if(cmd == "conditional_delete") {
        type = app::bridge::IndexingCommand::CONDITIONAL_DELETE;
    } else {
        throw utils::CException("invalid indexing command type");
    }
    return (breeze::app::bridge::IndexingCommand::Type)type;
}

void waiting_for_channel_to_ready(app::bridge::Client * bridge_client, std::pair<uint32_t, uint32_t>& cluster_key)
{
    while(true)
    {
        if(bridge_client->find_channel(cluster_key))
            break;
        sleep(1);
    }
}

void waiting_for_agent_to_ready(app::bridge::Client * bridge_client, std::pair<uint32_t, uint32_t>& cluster_key)
{
    while(true)
    {
        if(bridge_client->find_agent(cluster_key))
            break;
        sleep(1);
    }
}

std::string parameterize(const char *key, const char *value)
{
    utils::Json::Value param;
    param[key] = value;
    std::string jsoned_param = utils::Json::FastWriter().write(param);
    return jsoned_param;

}

int main(int argc, char **argv)
{
    utils::OptionParser parser;

    parser.add_option('g',  "group_id",                 NULL,   NULL,     "cluster group id");
    parser.add_option('c',  "cluster_id",               NULL,   NULL,     "cluster_id");
    parser.add_option('r',  "replica_id",               NULL,   NULL,     "replica_id");
    parser.add_option('p',  "pretty",                   NULL,   "false",  "pretty", "store_true");
    parser.add_option(0,    "conf",                     NULL,   NULL,     "breeze config file (breeze.cfg) Path");
    parser.add_option(0,    "cmd",                      NULL,   NULL,     "command\n\
        cmd argument can be one of:\n\
         analyzer_test, stat, get_schema, init_cluster, resume, suspend, warmup, indexing");
    parser.add_option(0,    "type",                     NULL,   NULL,     "indexing's sub command type\n\
        should specify this when cmd is 'indexing'\n\
        indexing operation type argument can be one of:\n\
         add_document, update, conditional_update, delete, conditional_delete");
    parser.add_option(0,    "schema",  NULL,   NULL,     "schema.xml file path");
    parser.add_option(0,    "file",                     NULL,   NULL,     "indexing data file path");
    parser.add_option(0,    "cdirective",               NULL,   NULL,     "user command directive file");
    parser.add_option(0,    "from_replica",             NULL,   NULL,     "from replica id");

    parser.add_option('s',  "save_addr",                NULL,   "false",  "save address to temporary file", "store_true");
    parser.add_option('a',  "addr",                     NULL,   "false",  "speicify address file", "store_true");

    parser.add_option(0,    "analyzer_name",            NULL,   NULL,     "analyzer_name");
    parser.add_option(0,    "analyzer_text",            NULL,   NULL,     "analyzer_text");
    parser.add_option(0,    "analyzer_option",          NULL,   NULL,     "analyzer_option");
    parser.parse(argc, argv);

    breeze::utils::init_logger_basic();


    int group_id, cluster_id;
    try {
        group_id = to_integer(parser.get_value("group_id"));
        cluster_id = to_integer(parser.get_value("cluster_id"));
    } catch (const utils::CException& ex) {
        std::cerr << "group_id and cluster_id should be specified as a integer type, " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::string breeze_cfg_path;
    if(not parser.get_value("conf", breeze_cfg_path))
        breeze_cfg_path = utils::join_path(PACKAGE_PREFIX, "conf/breeze.cfg");
    if(not utils::can_access(breeze_cfg_path, R_OK))  {
        std::cerr << "config file path error[" << breeze_cfg_path << "]" << std::endl;
        return EXIT_FAILURE;
    }

    // make Client
    ClientWrapper bridge_client_wrapper(breeze_cfg_path);
    app::bridge::Client *bridge_client = bridge_client_wrapper.get();

    if("false" == parser.get_value("addr"))
    {
        std::set<app::bridge::Cluster> clusters;
        clusters.insert(app::bridge::Cluster(group_id, cluster_id));
    }
    else
    {
        bridge_client->load_contact_list(TEMP_ADDRS);
    }

    std::pair<uint32_t, uint32_t> cluster_key = std::make_pair<uint32_t, uint32_t>(group_id, cluster_id);
    waiting_for_agent_to_ready(bridge_client, cluster_key);

    std::string str_cmd;
    if(not parser.get_value("cmd", str_cmd))
    {
        std::cerr << "cannot found command, should specify" << std::endl;
        return EXIT_FAILURE;
    }
    std::string cmd = utils::lower_string(str_cmd);

    std::string result("");

    try
    {
        if(cmd == "stat")
        {
            result = bridge_client->get_status(group_id, cluster_id);
        }
        else if(cmd == "analyzer_test")
        {
            std::string analyzer_name("");
            parser.get_value("analyzer_name", analyzer_name);
            if(analyzer_name.empty())
                analyzer_name = "dha_analyzer";

            std::string analyzer_text = parser.get_value("analyzer_text");
            if(analyzer_text.empty())  {
                std::cerr << "analyzer_text is empty." << std::endl;
                return EXIT_FAILURE;
            }

            std::string analyzer_option;
            try  {
                analyzer_option = parser.get_value("analyzer_option");
            }  catch(...)  {
                analyzer_option = "";
            }

            result = bridge_client->analyzer_test(group_id, cluster_id, analyzer_name, analyzer_text, analyzer_option);
        }
        else if(cmd == "init_cluster")
        {
            std::string schema = get_file_content(parser.get_value("schema"));
            bridge_client->init_cluster(group_id, cluster_id, schema);
        }
        else if(cmd == "resume")
        {
            bridge_client->resume(group_id, cluster_id);
        }
        else if(cmd == "suspend")
        {
            bridge_client->suspend(group_id, cluster_id);
        }
        else if(cmd == "prepare_recovery" || cmd == "prepare_recovery2")
        {
            utils::Json::Value param;
            param["replica_id"] = parser.get_value("replica_id");
            std::string content = utils::Json::FastWriter().write(param);
            result = bridge_client->post_control_command(group_id, cluster_id, cmd, content);
        }
        else if(cmd == "do_recovery" || cmd == "do_recovery2")
        {
            utils::Json::Value param;
            param["src_replica_id"] = parser.get_value("from_replica");
            param["dst_replica_id"] = parser.get_value("replica_id");

            std::string content = utils::Json::FastWriter().write(param);
            result = bridge_client->post_control_command(group_id, cluster_id, cmd, content);
        }
        else if(cmd == "indexing")
        {
            std::string cmdtype = parser.get_value("type");
            std::string data = get_file_content(parser.get_value("file"));

            waiting_for_channel_to_ready(bridge_client, cluster_key);

            Ice::Long sn  = bridge_client->post_indexing_command(group_id, cluster_id, indexing_type(cmdtype), data);
            std::cout << sn << std::endl;
        }
        else if(cmd == "get_schema")
        {
            result = bridge_client->get_schema(group_id, cluster_id);
        }
        else if(cmd == "warmup")
        {
            result = bridge_client->post_control_command(group_id, cluster_id, cmd, "");
        }
        else
        {
            if(cmd.empty())
            {
                std::string content = get_file_content(parser.get_value("cdirective"));
                result = bridge_client->post_control_command(group_id, cluster_id, cmd, content);
            }
            else
            {
                result = bridge_client->post_control_command(group_id, cluster_id, cmd, "");
            }
        }

        if(not result.empty())
        {
            if("true" == parser.get_value("pretty"))
            {
                utils::Json::Reader reader;
                utils::Json::Value v;
                if(!reader.parse(result, v))
                {
                    std::cerr << "fail to parse status data" << result << std::endl;
                    return EXIT_FAILURE;
                }
                result = utils::Json::StyledWriter().write(v);
            }
            std::cout << result << std::endl;
        }
    }
    catch(const utils::CException& ex)
    {
        std::cerr << "can't execute command " <<  cmd << ", " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch(const std::exception& ex)
    {
        std::cerr << "can't execute command " <<  cmd << ", " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch(...)
    {
        std::cerr << "can't execute command " << cmd << std::endl;
        return EXIT_FAILURE;
    }

    if("true" == parser.get_value("save_addr"))
        bridge_client->save_contact_list(TEMP_ADDRS);

    return EXIT_SUCCESS;
}
