#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Storages/StorageMaterializeMySQL.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <Processors/Pipe.h>
#include <Processors/Transforms/FilterTransform.h>

#include <Databases/MySQL/DatabaseMaterializeMySQL.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

StorageMaterializeMySQL::StorageMaterializeMySQL(const StoragePtr & nested_storage_, const IDatabase * database_)
    : StorageProxy(nested_storage_->getStorageID()), nested_storage(nested_storage_), database(database_)
{
    StorageInMemoryMetadata in_memory_metadata;
    in_memory_metadata = nested_storage->getInMemoryMetadata();
    setInMemoryMetadata(in_memory_metadata);
}

bool StorageMaterializeMySQL::needRewriteQueryWithFinal(const Names & column_names) const
{
    const StorageMetadataPtr & nested_metadata = nested_storage->getInMemoryMetadataPtr();
    Block nested_header = nested_metadata->getSampleBlock();
    ColumnWithTypeAndName & version_column = nested_header.getByPosition(nested_header.columns() - 1);
    return std::find(column_names.begin(), column_names.end(), version_column.name) == column_names.end();
}

Pipe StorageMaterializeMySQL::read(
        const Names & column_names, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info,
        ContextPtr context, QueryProcessingStage::Enum processed_stage, size_t max_block_size, unsigned num_streams)
{
    QueryPlan query_plan;
    read(query_plan, column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    return QueryPipeline::getPipe(std::move(*query_plan.buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context))));
}

void StorageMaterializeMySQL::read(
    QueryPlan & query_plan, const Names & column_names, const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & query_info, ContextPtr context, QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/, unsigned /*num_streams*/)
{
    /// If the background synchronization thread has exception.
    rethrowSyncExceptionIfNeed(database);

    NameSet column_names_set = NameSet(column_names.begin(), column_names.end());
    auto lock = nested_storage->lockForShare(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);
    const StorageMetadataPtr & nested_metadata = nested_storage->getInMemoryMetadataPtr();

    Block nested_header = nested_metadata->getSampleBlock();
    ColumnWithTypeAndName & sign_column = nested_header.getByPosition(nested_header.columns() - 2);
    ColumnWithTypeAndName & version_column = nested_header.getByPosition(nested_header.columns() - 1);

    //auto & select = query_info.query->as<ASTSelectQuery &>();
//
    //auto inner_select = std::make_shared<ASTSelectQuery>();
    //auto columns_list = std::make_shared<ASTExpressionList>();
    //for (const auto & column : column_names)
    //    columns_list->children.emplace_back(std::make_shared<ASTIdentifier>(column));
    //inner_select->setExpression(ASTSelectQuery::Expression::SELECT, columns_list);
    //inner_select->replaceDatabaseAndTable(getStorageID());
    //if (select.prewhere())
    //    inner_select->setExpression(ASTSelectQuery::Expression::PREWHERE, select.prewhere()->clone());
    //if (select.where())
    //    inner_select->setExpression(ASTSelectQuery::Expression::WHERE, select.where()->clone());

    SelectQueryInfo modified_query_info = query_info;
    modified_query_info.query = query_info.query->clone();

    /// Original query could contain JOIN but we need only the first joined table and its columns.
    auto & modified_select = modified_query_info.query->as<ASTSelectQuery &>();

    TreeRewriterResult new_analyzer_res = *query_info.syntax_analyzer_result;
    removeJoin(modified_select, new_analyzer_res, context);


    auto inner_select = std::make_shared<ASTSelectQuery>();
    auto columns_list = std::make_shared<ASTExpressionList>();
    for (const auto & column : column_names)
        columns_list->children.emplace_back(std::make_shared<ASTIdentifier>(column));
    inner_select->setExpression(ASTSelectQuery::Expression::SELECT, columns_list);
    inner_select->replaceDatabaseAndTable(getStorageID());
    if (modified_select.prewhere())
        inner_select->setExpression(ASTSelectQuery::Expression::PREWHERE, modified_select.prewhere()->clone());
    if (modified_select.where())
        inner_select->setExpression(ASTSelectQuery::Expression::WHERE, modified_select.where()->clone());


    /// Add FINAL modifier to SELECT query is version column is not used in query explicitly.
    if (!column_names_set.count(version_column.name))
    {
        auto & tables_in_select_query = inner_select->tables()->as<ASTTablesInSelectQuery &>();

        if (!tables_in_select_query.children.empty())
        {
            auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();

            if (tables_element.table_expression)
                tables_element.table_expression->as<ASTTableExpression &>().final = true;
        }
        assert(inner_select->final());
    }

    if (!column_names_set.count(sign_column.name))
    {
        const auto & sign_column_name = std::make_shared<ASTIdentifier>(sign_column.name);
        const auto & fetch_sign_value = std::make_shared<ASTLiteral>(Field(Int8(1)));
        auto sign_condition = makeASTFunction("equals", sign_column_name, fetch_sign_value);
        auto new_where = sign_condition;
        if (inner_select->where())
            new_where = makeASTFunction("and", sign_condition, inner_select->where());
        inner_select->setExpression(ASTSelectQuery::Expression::WHERE, new_where);
    }

    //auto stage = nested_storage->getQueryProcessingStage(context, processed_stage, nested_metadata, modified_query_info);
    InterpreterSelectQuery interpreter{inner_select, context, nested_storage, nested_metadata, SelectQueryOptions(QueryProcessingStage::WithMergeableState)};
    interpreter.buildQueryPlan(query_plan);
}

//Pipe StorageMaterializeMySQL::read(
//    const Names & column_names,
//    const StorageMetadataPtr & /*metadata_snapshot*/,
//    SelectQueryInfo & query_info,
//    ContextPtr context,
//    QueryProcessingStage::Enum processed_stage,
//    size_t /*max_block_size*/,
//    unsigned int /*num_streams*/)
//{
//    /// If the background synchronization thread has exception.
//    rethrowSyncExceptionIfNeed(database);
//
//    NameSet column_names_set = NameSet(column_names.begin(), column_names.end());
//    auto lock = nested_storage->lockForShare(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);
//    const StorageMetadataPtr & nested_metadata = nested_storage->getInMemoryMetadataPtr();
//
//    Block nested_header = nested_metadata->getSampleBlock();
//    ColumnWithTypeAndName & sign_column = nested_header.getByPosition(nested_header.columns() - 2);
//    ColumnWithTypeAndName & version_column = nested_header.getByPosition(nested_header.columns() - 1);
//
//    SelectQueryInfo modified_query_info = query_info;
//    modified_query_info.query = query_info.query->clone();
//
//    /// Original query could contain JOIN but we need only the first joined table and its columns.
//    auto & modified_select = modified_query_info.query->as<ASTSelectQuery &>();
//
//    TreeRewriterResult new_analyzer_res = *query_info.syntax_analyzer_result;
//    removeJoin(modified_select, new_analyzer_res, context);
//
//    /// Add FINAL modifier to SELECT query is version column is not used in query explicitly.
//    if (!column_names_set.count(version_column.name))
//    {
//        auto & tables_in_select_query = modified_select.tables()->as<ASTTablesInSelectQuery &>();
//
//        if (!tables_in_select_query.children.empty())
//        {
//            auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
//
//            if (tables_element.table_expression)
//                tables_element.table_expression->as<ASTTableExpression &>().final = true;
//        }
//        assert(modified_select.final());
//    }
//
//    //if (!column_names_set.count(sign_column.name))
//    //{
//    //    const auto & sign_column_name = std::make_shared<ASTIdentifier>(sign_column.name);
//    //    const auto & fetch_sign_value = std::make_shared<ASTLiteral>(Field(Int8(1)));
//    //    auto sign_condition = makeASTFunction("equals", sign_column_name, fetch_sign_value);
//    //    auto new_where = sign_condition;
//    //    if (modified_select.where())
//    //        new_where = makeASTFunction("and", sign_condition, modified_select.where());
//    //    modified_select.setExpression(ASTSelectQuery::Expression::WHERE, new_where);
//    //}
//
//    String filter_column_name;
//    //Names require_columns_name = column_names;
//    ASTPtr expressions = std::make_shared<ASTExpressionList>();
//    if (!column_names_set.count(sign_column.name))
//    {
//        //require_columns_name.emplace_back(sign_column.name);
//
//        const auto & sign_column_name = std::make_shared<ASTIdentifier>(sign_column.name);
//        const auto & fetch_sign_value = std::make_shared<ASTLiteral>(Field(Int8(1)));
//
//        expressions->children.emplace_back(makeASTFunction("equals", sign_column_name, fetch_sign_value));
//        filter_column_name = expressions->children.back()->getColumnName();
//        modified_select.refSelect()->children.emplace_back(std::make_shared<ASTIdentifier>(sign_column.name));
//    }
//
//    //auto stage = nested_storage->getQueryProcessingStage(context, processed_stage, nested_metadata, modified_query_info);
//    InterpreterSelectQuery interpreter{modified_query_info.query, context, nested_storage, nested_metadata, SelectQueryOptions(processed_stage)};
//    Pipe pipe = QueryPipeline::getPipe(interpreter.execute().pipeline);
//    pipe.addTableLock(lock);
//
//    //Pipe pipe = nested_storage->read(require_columns_name, nested_metadata, query_info, context, processed_stage, max_block_size, num_streams);
//    //pipe.addTableLock(lock);
//
//    if (!expressions->children.empty() && !pipe.empty())
//    {
//        Block pipe_header = pipe.getHeader();
//        auto syntax = TreeRewriter(context).analyze(expressions, pipe_header.getNamesAndTypesList());
//        ExpressionActionsPtr expression_actions = ExpressionAnalyzer(expressions, syntax, context).getActions(true /* add_aliases */, false /* project_result */);
//
//        pipe.addSimpleTransform([&](const Block & header)
//        {
//            return std::make_shared<FilterTransform>(header, expression_actions, filter_column_name, false);
//        });
//    }
//
//    return pipe;
//}

NamesAndTypesList StorageMaterializeMySQL::getVirtuals() const
{
    /// If the background synchronization thread has exception.
    rethrowSyncExceptionIfNeed(database);
    return nested_storage->getVirtuals();
}

IStorage::ColumnSizeByName StorageMaterializeMySQL::getColumnSizes() const
{
    auto sizes = nested_storage->getColumnSizes();
    auto nested_header = nested_storage->getInMemoryMetadataPtr()->getSampleBlock();
    String sign_column_name = nested_header.getByPosition(nested_header.columns() - 2).name;
    String version_column_name = nested_header.getByPosition(nested_header.columns() - 1).name;
    sizes.erase(sign_column_name);
    sizes.erase(version_column_name);
    return sizes;
}

}

#endif
