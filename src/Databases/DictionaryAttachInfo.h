#pragma once

#include <Parsers/IAST_fwd.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>


namespace DB
{

struct DictionaryAttachInfo
{
    DictionaryAttachInfo(const ASTPtr & create)
    : create_query(create)
    , config(getDictionaryConfigurationFromAST(query->as<const ASTCreateQuery &>()))
    , modification_time(nullptr)
    {
    }

    ASTPtr create_query;
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config;
    time_t modification_time;
};

}
