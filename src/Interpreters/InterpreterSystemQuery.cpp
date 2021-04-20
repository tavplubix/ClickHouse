#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Common/thread_local_rng.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Common/ShellCommand.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ExpressionJIT.h>
#include <Access/ContextAccess.h>
#include <Access/AllowedClientHosts.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageFactory.h>
#include "Core/QueryProcessingStage.h"
#include "Interpreters/executeQuery.h"
#include "Parsers/ParserAlterQuery.h"
#include "Parsers/ParserDropQuery.h"
#include "Parsers/ParserRenameQuery.h"
#include "Parsers/ParserSelectQuery.h"
#include "Parsers/ParserSystemQuery.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
#include "Storages/System/StorageSystemParts.h"

#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Poco/DirectoryIterator.h>

#include <csignal>
#include <algorithm>
#include <memory>
#include <filesystem>
#include <system_error>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int SYSTEM_ERROR;
    extern const int NO_ZOOKEEPER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


namespace ActionLocks
{
    extern StorageActionBlockType PartsMerge;
    extern StorageActionBlockType PartsFetch;
    extern StorageActionBlockType PartsSend;
    extern StorageActionBlockType ReplicationQueue;
    extern StorageActionBlockType DistributedSend;
    extern StorageActionBlockType PartsTTLMerge;
    extern StorageActionBlockType PartsMove;
}


namespace
{

ExecutionStatus getOverallExecutionStatusOfCommands()
{
    return ExecutionStatus(0);
}

/// Consequently tries to execute all commands and generates final exception message for failed commands
template <typename Callable, typename ... Callables>
ExecutionStatus getOverallExecutionStatusOfCommands(Callable && command, Callables && ... commands)
{
    ExecutionStatus status_head(0);
    try
    {
        command();
    }
    catch (...)
    {
        status_head = ExecutionStatus::fromCurrentException();
    }

    ExecutionStatus status_tail = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);

    auto res_status = status_head.code != 0 ? status_head.code : status_tail.code;
    auto res_message = status_head.message + (status_tail.message.empty() ? "" : ("\n" + status_tail.message));

    return ExecutionStatus(res_status, res_message);
}

/// Consequently tries to execute all commands and throws exception with info about failed commands
template <typename ... Callables>
void executeCommandsAndThrowIfError(Callables && ... commands)
{
    auto status = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);
    if (status.code != 0)
        throw Exception(status.message, status.code);
}


AccessType getRequiredAccessType(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return AccessType::SYSTEM_MERGES;
    else if (action_type == ActionLocks::PartsFetch)
        return AccessType::SYSTEM_FETCHES;
    else if (action_type == ActionLocks::PartsSend)
        return AccessType::SYSTEM_REPLICATED_SENDS;
    else if (action_type == ActionLocks::ReplicationQueue)
        return AccessType::SYSTEM_REPLICATION_QUEUES;
    else if (action_type == ActionLocks::DistributedSend)
        return AccessType::SYSTEM_DISTRIBUTED_SENDS;
    else if (action_type == ActionLocks::PartsTTLMerge)
        return AccessType::SYSTEM_TTL_MERGES;
    else if (action_type == ActionLocks::PartsMove)
        return AccessType::SYSTEM_MOVES;
    else
        throw Exception("Unknown action type: " + std::to_string(action_type), ErrorCodes::LOGICAL_ERROR);
}

}

/// Implements SYSTEM [START|STOP] <something action from ActionLocks>
void InterpreterSystemQuery::startStopAction(StorageActionBlockType action_type, bool start)
{
    auto manager = getContext()->getActionLocksManager();
    manager->cleanExpired();

    if (volume_ptr && action_type == ActionLocks::PartsMerge)
    {
        volume_ptr->setAvoidMergesUserOverride(!start);
    }
    else if (table_id)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (table)
        {
            if (start)
            {
                manager->remove(table, action_type);
                table->onActionLockRemove(action_type);
            }
            else
                manager->add(table, action_type);
        }
    }
    else
    {
        auto access = getContext()->getAccess();
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            for (auto iterator = elem.second->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                StoragePtr table = iterator->table();
                if (!table)
                    continue;

                if (!access->isGranted(getRequiredAccessType(action_type), elem.first, iterator->name()))
                {
                    LOG_INFO(
                        log,
                        "Access {} denied, skipping {}.{}",
                        toString(getRequiredAccessType(action_type)),
                        elem.first,
                        iterator->name());
                    continue;
                }

                if (start)
                {
                    manager->remove(table, action_type);
                    table->onActionLockRemove(action_type);
                }
                else
                    manager->add(table, action_type);
            }
        }
    }
}


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithContext(context_), query_ptr(query_ptr_->clone()), log(&Poco::Logger::get("InterpreterSystemQuery"))
{
}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = query_ptr->as<ASTSystemQuery &>();

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext(), getRequiredAccessForDDLOnCluster());

    using Type = ASTSystemQuery::Type;

    /// Use global context with fresh system profile settings
    auto system_context = Context::createCopy(getContext()->getGlobalContext());
    system_context->setSetting("profile", getContext()->getSystemProfileName());

    /// Make canonical query for simpler processing
    if (!query.table.empty())
        table_id = getContext()->resolveStorageID(StorageID(query.database, query.table), Context::ResolveOrdinary);

    if (!query.target_dictionary.empty() && !query.database.empty())
        query.target_dictionary = query.database + "." + query.target_dictionary;

    volume_ptr = {};
    if (!query.storage_policy.empty() && !query.volume.empty())
        volume_ptr = getContext()->getStoragePolicy(query.storage_policy)->getVolumeByName(query.volume);

    switch (query.type)
    {
        case Type::SHUTDOWN:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            if (kill(0, SIGTERM))
                throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
            break;
        }
        case Type::KILL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
            /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
            LOG_INFO(log, "Exit immediately as the SYSTEM KILL command has been issued.");
            _exit(128 + SIGKILL);
            // break; /// unreachable
        }
        case Type::SUSPEND:
        {
            auto command = fmt::format("kill -STOP {0} && sleep {1} && kill -CONT {0}", getpid(), query.seconds);
            LOG_DEBUG(log, "Will run {}", command);
            auto res = ShellCommand::execute(command);
            res->in.close();
            WriteBufferFromOwnString out;
            copyData(res->out, out);
            copyData(res->err, out);
            if (!out.str().empty())
                LOG_DEBUG(log, "The command returned output: {}", command, out.str());
            res->wait();
            break;
        }
        case Type::DROP_DNS_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_DNS_CACHE);
            DNSResolver::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            system_context->reloadClusterConfig();
            break;
        }
        case Type::DROP_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->dropUncompressedCache();
            break;
        case Type::DROP_MMAP_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MMAP_CACHE);
            system_context->dropMMappedFileCache();
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE);
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->reset();
            break;
#endif
        case Type::RELOAD_DICTIONARY:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);

            auto & external_dictionaries_loader = system_context->getExternalDictionariesLoader();
            external_dictionaries_loader.reloadDictionary(query.target_dictionary, getContext());


            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_DICTIONARIES:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);
            executeCommandsAndThrowIfError(
                    [&] () { system_context->getExternalDictionariesLoader().reloadAllTriedToLoad(); },
                    [&] () { system_context->getEmbeddedDictionaries().reload(); }
            );
            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_MODEL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto & external_models_loader = system_context->getExternalModelsLoader();
            external_models_loader.reloadModel(query.target_model);
            break;
        }
        case Type::RELOAD_MODELS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto & external_models_loader = system_context->getExternalModelsLoader();
            external_models_loader.reloadAllTriedToLoad();
            break;
        }
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES);
            system_context->getEmbeddedDictionaries().reload();
            break;
        case Type::RELOAD_CONFIG:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            system_context->reloadConfig();
            break;
        case Type::RELOAD_SYMBOLS:
        {
#if defined(__ELF__) && !defined(__FreeBSD__)
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_SYMBOLS);
            (void)SymbolIndex::instance(true);
            break;
#else
            throw Exception("SYSTEM RELOAD SYMBOLS is not supported on current platform", ErrorCodes::NOT_IMPLEMENTED);
#endif
        }
        case Type::STOP_MERGES:
            startStopAction(ActionLocks::PartsMerge, false);
            break;
        case Type::START_MERGES:
            startStopAction(ActionLocks::PartsMerge, true);
            break;
        case Type::STOP_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, false);
            break;
        case Type::START_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, true);
            break;
        case Type::STOP_MOVES:
            startStopAction(ActionLocks::PartsMove, false);
            break;
        case Type::START_MOVES:
            startStopAction(ActionLocks::PartsMove, true);
            break;
        case Type::STOP_FETCHES:
            startStopAction(ActionLocks::PartsFetch, false);
            break;
        case Type::START_FETCHES:
            startStopAction(ActionLocks::PartsFetch, true);
            break;
        case Type::STOP_REPLICATED_SENDS:
            startStopAction(ActionLocks::PartsSend, false);
            break;
        case Type::START_REPLICATED_SENDS:
            startStopAction(ActionLocks::PartsSend, true);
            break;
        case Type::STOP_REPLICATION_QUEUES:
            startStopAction(ActionLocks::ReplicationQueue, false);
            break;
        case Type::START_REPLICATION_QUEUES:
            startStopAction(ActionLocks::ReplicationQueue, true);
            break;
        case Type::STOP_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, false);
            break;
        case Type::START_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, true);
            break;
        case Type::DROP_REPLICA:
            dropReplica(query);
            break;
        case Type::SYNC_REPLICA:
            syncReplica(query);
            break;
        case Type::FLUSH_DISTRIBUTED:
            flushDistributed(query);
            break;
        case Type::RESTART_REPLICAS:
            restartReplicas(system_context);
            break;
        case Type::RESTART_REPLICA:
            if (!tryRestartReplica(table_id, system_context))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no replicated table {}.{}",
                    query.database, query.table);
            break;
        case Type::RESTORE_REPLICA:
            restoreReplica(system_context);
            break;
        case Type::FLUSH_LOGS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FLUSH_LOGS);
            executeCommandsAndThrowIfError(
                [&] { if (auto query_log = getContext()->getQueryLog()) query_log->flush(true); },
                [&] { if (auto part_log = getContext()->getPartLog("")) part_log->flush(true); },
                [&] { if (auto query_thread_log = getContext()->getQueryThreadLog()) query_thread_log->flush(true); },
                [&] { if (auto trace_log = getContext()->getTraceLog()) trace_log->flush(true); },
                [&] { if (auto text_log = getContext()->getTextLog()) text_log->flush(true); },
                [&] { if (auto metric_log = getContext()->getMetricLog()) metric_log->flush(true); },
                [&] { if (auto asynchronous_metric_log = getContext()->getAsynchronousMetricLog()) asynchronous_metric_log->flush(true); },
                [&] { if (auto opentelemetry_span_log = getContext()->getOpenTelemetrySpanLog()) opentelemetry_span_log->flush(true); }
            );
            break;
        }
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
            throw Exception(String(ASTSystemQuery::typeToString(query.type)) + " is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
        default:
            throw Exception("Unknown type of SYSTEM query", ErrorCodes::BAD_ARGUMENTS);
    }

    return BlockIO();
}

Strings InterpreterSystemQuery::movePartsToNewTableDetachedFolder(const String& db_name, const String& new_table_name)
{
    Strings parts_names;

    auto new_table_ptr = DatabaseCatalog::instance().getTable({db_name, new_table_name}, getContext());
    auto& storage = *static_cast<StorageReplicatedMergeTree*>(new_table_ptr.get());

    const auto active_parts = storage.getDataPartsVector();
    parts_names.reserve(active_parts.size());

    for (const auto& part : active_parts)
    {
        const ReservationPtr reservation = storage.reserveSpace(part->getBytesOnDisk());
        const String part_target_path = storage.getFullPathOnDisk(reservation->getDisk()) + "detached/" + part->name;

        std::error_code error;
        std::filesystem::rename(part->getFullPath(), part_target_path, error);

        if (error)
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Error moving part from {} to {}: {}",
                part->getFullPath(), part_target_path, error.message());

        parts_names.emplace_back(part->name);
    }

    return parts_names;
}

InterpreterSystemQuery::ReplicaAndZK InterpreterSystemQuery::checkTablesAndSwapIfNeeded(
    const String& db_name, const String& old_table_name, const String& new_table_name) const
{
    const StoragePtr table = DatabaseCatalog::instance().getTable({db_name, old_table_name}, getContext());
    StorageReplicatedMergeTree *old_table;

    if (!table || ((old_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get())) == nullptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no replicated table \"{}.{}\"", db_name, old_table_name);

    const String replica_name = old_table->getReplicaName();

    /**
     * What if server failed while processing the query?
     * - If failed before step 4, everything is ok.
     * - If failed after, everything would break down when trying to restart the restore query, so we need to check
     *   it and rename tables if needed.
     */
    StorageReplicatedMergeTree::Status status;
    old_table->getStatus(status);

    if (auto new_table_ptr = DatabaseCatalog::instance().tryGetTable({db_name, new_table_name}, getContext());
        new_table_ptr && !status.is_readonly)
    {
        /// Both of the tables are present, and the table with old_table_name is not readonly, so we need to rename
        assert(static_cast<StorageReplicatedMergeTree*>(new_table_ptr.get())->isReadonly());
        LOG_DEBUG(log, "Looks like server failed while processing the query, renaming the tables");

        executeQuery(fmt::format("EXCHANGE TABLES {0}.{1} AND {0}.{2}", db_name, new_table_name, old_table_name),
            getContext(), true);

        /// Now the old_table_name is really the old table.
        static_cast<StorageReplicatedMergeTree*>(
            DatabaseCatalog::instance().getTable({db_name, old_table_name}, getContext()).get())->getStatus(status);
    }

    return {replica_name, status.zookeeper_path};
}

void InterpreterSystemQuery::restoreReplica(ContextPtr system_context)
{
    /**
     * This query should be:
     * - idempotent on non-existent or invalid table invocation (i.e. throwing).
     * - re-invokable (i.e. when the server failed while processing RESTORE REPLICA query).
     *   The server should correctly restore the replica regardless of the step where the last invocation failed.
     *
     * So no random may be used.
     * NOTE: The integration test, checking the re-invocation, kills server after finding certain messages in log,
     * so, if changing the latter, also change the test (test_restore_replica).
     */

    getContext()->checkAccess(AccessType::SYSTEM_RESTORE_REPLICA, table_id);

    const zkutil::ZooKeeperPtr& zookeeper = getContext()->getZooKeeper();

    if (zookeeper->expired())
        throw Exception(ErrorCodes::NO_ZOOKEEPER,
            "Cannot restore table metadata because ZooKeeper session has expired");

    const String& db_name = table_id.database_name;
    const String& old_table_name = table_id.table_name;
    const std::hash<String> table_name_hash;
    const String new_table_name = fmt::format("{}_tmp_{}", old_table_name, table_name_hash(old_table_name));

    const auto [replica_name, zk_root_path] = checkTablesAndSwapIfNeeded(db_name, old_table_name, new_table_name);

    if (const String replicas_zk_path = zk_root_path + "/replicas"; zookeeper->exists(replicas_zk_path))
    {
        if (const String replica_zk_path = replicas_zk_path + "/" + replica_name; zookeeper->exists(replica_zk_path))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The metadata for {}.{} (replica {}) is present at {} -- nothing to restore",
                db_name, old_table_name, replica_name, replica_zk_path);
        }

        Strings replicas_present;
        zookeeper->tryGetChildren(replicas_zk_path, replicas_present);

        if (!replicas_present.empty()) // at least one table has valid metadata in zk
        {
            LOG_DEBUG(log, "At least one replica is present, restoring state from it");
            tryRestartReplica(table_id, system_context); // Runs table attach which will auto restore the ZK metadata.
            return;
        }

        LOG_DEBUG(log, "Replicas path is empty");
    }

    LOG_DEBUG(log, "Started restoring {}.{}, zk root path at {}", db_name, old_table_name, zk_root_path);

    /// 1.
    /// If the server failed after this step, the old temporary table won't be lost in mem so the query re-run could
    /// succeed (need of IF NOT EXISTS).
    executeQuery(fmt::format("CREATE TABLE IF NOT EXISTS {0}.{1} AS {0}.{2}", db_name, new_table_name, old_table_name),
        getContext(), true);

    LOG_DEBUG(log, "Created a new replicated table {}.{}", db_name, new_table_name);

    /// 2.
    executeQuery(fmt::format("SYSTEM STOP FETCHES {}.{}", db_name, old_table_name), getContext(), true);
    LOG_DEBUG(log, "Stopped replica fetches for {}.{}", db_name, old_table_name);

    /// 3. Register parts in new table and send data to ZooKeeper.
    for (const String& part_name : movePartsToNewTableDetachedFolder(db_name, new_table_name))
        executeQuery(fmt::format("ALTER TABLE {}.{} ATTACH PART '{}'", db_name, new_table_name, part_name),
            getContext(), true);

    LOG_DEBUG(log, "Moved and attached all parts from {0}.{1} to {0}.{2}", db_name, old_table_name, new_table_name);

    /// 4.
    executeQuery(
        fmt::format("EXCHANGE TABLES {0}.{1} AND {0}.{2}", db_name, new_table_name, old_table_name),
        getContext(), true);

    LOG_DEBUG(log, "Renamed tables {0}.{1} <=> {0}.{2}", db_name, old_table_name, new_table_name);

    /// 5.
    executeQuery(fmt::format("DETACH TABLE {}.{}", db_name, new_table_name), getContext(), true);
    LOG_DEBUG(log, "Detached old table {}.{}", db_name, new_table_name);

    /// 6. Delete information about the old table, so it wouldn't be attached after server restart.
    const DatabasePtr db = DatabaseCatalog::instance().getDatabase(db_name, getContext());
    const String old_table_metadata_file = db->getObjectMetadataPath(new_table_name);

    if (auto ec = std::error_code{}; !std::filesystem::remove(old_table_metadata_file, ec))
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Error removing file {}: {}",
            old_table_metadata_file, ec.message());

    LOG_DEBUG(log, "Removed old table {}.{} metadata at {}", db_name, new_table_name, old_table_metadata_file);
}

StoragePtr InterpreterSystemQuery::tryRestartReplica(const StorageID & replica, ContextPtr system_context, bool need_ddl_guard)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_REPLICA, replica);

    auto table_ddl_guard = need_ddl_guard
        ? DatabaseCatalog::instance().getDDLGuard(replica.getDatabaseName(), replica.getTableName())
        : nullptr;

    auto [database, table] = DatabaseCatalog::instance().tryGetDatabaseAndTable(replica, getContext());
    ASTPtr create_ast;

    /// Detach actions
    if (!table || !dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
        return nullptr;

    table->shutdown();
    {
        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table->lockExclusively(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
        create_ast = database->getCreateTableQuery(replica.table_name, getContext());

        database->detachTable(replica.table_name);
    }
    table.reset();

    /// Attach actions
    /// getCreateTableQuery must return canonical CREATE query representation, there are no need for AST postprocessing
    auto & create = create_ast->as<ASTCreateQuery &>();
    create.attach = true;

    auto columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, system_context, true);
    auto constraints = InterpreterCreateQuery::getConstraintsDescription(create.columns_list->constraints);
    auto data_path = database->getTableDataPath(create);

    table = StorageFactory::instance().get(create,
        data_path,
        system_context,
        system_context->getGlobalContext(),
        columns,
        constraints,
        false);

    database->attachTable(replica.table_name, table, data_path);

    table->startup();
    return table;
}

void InterpreterSystemQuery::restartReplicas(ContextPtr system_context)
{
    std::vector<StorageID> replica_names;
    auto & catalog = DatabaseCatalog::instance();

    for (auto & elem : catalog.getDatabases())
    {
        DatabasePtr & database = elem.second;
        for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
        {
            if (auto table = iterator->table())
            {
                if (dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
                    replica_names.emplace_back(StorageID{iterator->databaseName(), iterator->name()});
            }
        }
    }

    if (replica_names.empty())
        return;

    TableGuards guards;
    for (const auto & name : replica_names)
        guards.emplace(UniqueTableName{name.database_name, name.table_name}, nullptr);
    for (auto & guard : guards)
        guard.second = catalog.getDDLGuard(guard.first.database_name, guard.first.table_name);

    ThreadPool pool(std::min(size_t(getNumberOfPhysicalCPUCores()), replica_names.size()));
    for (auto & replica : replica_names)
    {
        LOG_TRACE(log, "Restarting replica on {}", replica.getNameForLogs());
        pool.scheduleOrThrowOnError([&]() { tryRestartReplica(replica, system_context, false); });
    }
    pool.wait();
}

void InterpreterSystemQuery::dropReplica(ASTSystemQuery & query)
{
    if (query.replica.empty())
        throw Exception("Replica name is empty", ErrorCodes::BAD_ARGUMENTS);

    if (!table_id.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, table_id);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

        if (!dropReplicaImpl(query, table))
            throw Exception("Table " + table_id.getNameForLogs() + " is not replicated", ErrorCodes::BAD_ARGUMENTS);
    }
    else if (!query.database.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, query.database);
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(query.database);
        for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            dropReplicaImpl(query, iterator->table());
        LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
    }
    else if (query.is_drop_whole_replica)
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA);
        auto databases = DatabaseCatalog::instance().getDatabases();

        for (auto & elem : databases)
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
                dropReplicaImpl(query, iterator->table());
            LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
        }
    }
    else if (!query.replica_zk_path.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA);
        auto remote_replica_path = query.replica_zk_path  + "/replicas/" + query.replica;

        /// This check is actually redundant, but it may prevent from some user mistakes
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(iterator->table().get()))
                {
                    StorageReplicatedMergeTree::Status status;
                    storage_replicated->getStatus(status);
                    if (status.zookeeper_path == query.replica_zk_path)
                        throw Exception("There is a local table " + storage_replicated->getStorageID().getNameForLogs() +
                                        ", which has the same table path in ZooKeeper. Please check the path in query. "
                                        "If you want to drop replica of this table, use `DROP TABLE` "
                                        "or `SYSTEM DROP REPLICA 'name' FROM db.table`", ErrorCodes::TABLE_WAS_NOT_DROPPED);
                }
            }
        }

        auto zookeeper = getContext()->getZooKeeper();

        bool looks_like_table_path = zookeeper->exists(query.replica_zk_path + "/replicas") ||
                                     zookeeper->exists(query.replica_zk_path + "/dropped");
        if (!looks_like_table_path)
            throw Exception("Specified path " + query.replica_zk_path + " does not look like a table path",
                            ErrorCodes::TABLE_WAS_NOT_DROPPED);

        if (zookeeper->exists(remote_replica_path + "/is_active"))
            throw Exception("Can't remove replica: " + query.replica + ", because it's active",
                ErrorCodes::TABLE_WAS_NOT_DROPPED);

        StorageReplicatedMergeTree::dropReplica(zookeeper, query.replica_zk_path, query.replica, log);
        LOG_INFO(log, "Dropped replica {}", remote_replica_path);
    }
    else
        throw Exception("Invalid query", ErrorCodes::LOGICAL_ERROR);
}

bool InterpreterSystemQuery::dropReplicaImpl(ASTSystemQuery & query, const StoragePtr & table)
{
    auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
    if (!storage_replicated)
        return false;

    StorageReplicatedMergeTree::Status status;
    auto zookeeper = getContext()->getZooKeeper();
    storage_replicated->getStatus(status);

    /// Do not allow to drop local replicas and active remote replicas
    if (query.replica == status.replica_name)
        throw Exception("We can't drop local replica, please use `DROP TABLE` "
                        "if you want to clean the data and drop this replica", ErrorCodes::TABLE_WAS_NOT_DROPPED);

    /// NOTE it's not atomic: replica may become active after this check, but before dropReplica(...)
    /// However, the main usecase is to drop dead replica, which cannot become active.
    /// This check prevents only from accidental drop of some other replica.
    if (zookeeper->exists(status.zookeeper_path + "/replicas/" + query.replica + "/is_active"))
        throw Exception("Can't drop replica: " + query.replica + ", because it's active",
                        ErrorCodes::TABLE_WAS_NOT_DROPPED);

    storage_replicated->dropReplica(zookeeper, status.zookeeper_path, query.replica, log);
    LOG_TRACE(log, "Dropped replica {} of {}", query.replica, table->getStorageID().getNameForLogs());

    return true;
}

void InterpreterSystemQuery::syncReplica(ASTSystemQuery &)
{
    getContext()->checkAccess(AccessType::SYSTEM_SYNC_REPLICA, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in replica's queue with table's log and waiting for it to become empty");
        if (!storage_replicated->waitForShrinkingQueueSize(0, getContext()->getSettingsRef().receive_timeout.totalMilliseconds()))
        {
            LOG_ERROR(log, "SYNC REPLICA {}: Timed out!", table_id.getNameForLogs());
            throw Exception(
                    "SYNC REPLICA " + table_id.getNameForLogs() + ": command timed out! "
                    "See the 'receive_timeout' setting", ErrorCodes::TIMEOUT_EXCEEDED);
        }
        LOG_TRACE(log, "SYNC REPLICA {}: OK", table_id.getNameForLogs());
    }
    else
        throw Exception("Table " + table_id.getNameForLogs() + " is not replicated", ErrorCodes::BAD_ARGUMENTS);
}

void InterpreterSystemQuery::flushDistributed(ASTSystemQuery &)
{
    getContext()->checkAccess(AccessType::SYSTEM_FLUSH_DISTRIBUTED, table_id);

    if (auto * storage_distributed = dynamic_cast<StorageDistributed *>(DatabaseCatalog::instance().getTable(table_id, getContext()).get()))
        storage_distributed->flushClusterNodesAllData(getContext());
    else
        throw Exception("Table " + table_id.getNameForLogs() + " is not distributed", ErrorCodes::BAD_ARGUMENTS);
}


AccessRightsElements InterpreterSystemQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<const ASTSystemQuery &>();
    using Type = ASTSystemQuery::Type;
    AccessRightsElements required_access;

    switch (query.type)
    {
        case Type::SHUTDOWN: [[fallthrough]];
        case Type::KILL: [[fallthrough]];
        case Type::SUSPEND:
        {
            required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
            break;
        }
        case Type::DROP_DNS_CACHE: [[fallthrough]];
        case Type::DROP_MARK_CACHE: [[fallthrough]];
        case Type::DROP_MMAP_CACHE: [[fallthrough]];
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE: [[fallthrough]];
#endif
        case Type::DROP_UNCOMPRESSED_CACHE:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_CACHE);
            break;
        }
        case Type::RELOAD_DICTIONARY: [[fallthrough]];
        case Type::RELOAD_DICTIONARIES: [[fallthrough]];
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_DICTIONARY);
            break;
        }
        case Type::RELOAD_MODEL: [[fallthrough]];
        case Type::RELOAD_MODELS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_MODEL);
            break;
        }
        case Type::RELOAD_CONFIG:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_CONFIG);
            break;
        }
        case Type::RELOAD_SYMBOLS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_SYMBOLS);
            break;
        }
        case Type::STOP_MERGES: [[fallthrough]];
        case Type::START_MERGES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_TTL_MERGES: [[fallthrough]];
        case Type::START_TTL_MERGES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_MOVES: [[fallthrough]];
        case Type::START_MOVES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_MOVES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MOVES, query.database, query.table);
            break;
        }
        case Type::STOP_FETCHES: [[fallthrough]];
        case Type::START_FETCHES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_FETCHES);
            else
                required_access.emplace_back(AccessType::SYSTEM_FETCHES, query.database, query.table);
            break;
        }
        case Type::STOP_DISTRIBUTED_SENDS: [[fallthrough]];
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS, query.database, query.table);
            break;
        }
        case Type::STOP_REPLICATED_SENDS: [[fallthrough]];
        case Type::START_REPLICATED_SENDS:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS, query.database, query.table);
            break;
        }
        case Type::STOP_REPLICATION_QUEUES: [[fallthrough]];
        case Type::START_REPLICATION_QUEUES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES, query.database, query.table);
            break;
        }
        case Type::DROP_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTORE_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTORE_REPLICA, query.database, query.table);
            break;
        }
        case Type::SYNC_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_SYNC_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTART_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTART_REPLICAS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA);
            break;
        }
        case Type::FLUSH_DISTRIBUTED:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_DISTRIBUTED, query.database, query.table);
            break;
        }
        case Type::FLUSH_LOGS:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_LOGS);
            break;
        }
        case Type::STOP_LISTEN_QUERIES: break;
        case Type::START_LISTEN_QUERIES: break;
        case Type::UNKNOWN: break;
        case Type::END: break;
    }
    return required_access;
}

void InterpreterSystemQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr) const
{
    elem.query_kind = "System";
}

}
