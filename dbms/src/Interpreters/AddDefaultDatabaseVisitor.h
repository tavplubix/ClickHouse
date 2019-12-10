#pragma once

#include <Common/typeid_cast.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/DumpASTNode.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>

namespace DB
{

/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and function could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/// Visits AST nodes, add default database to tables if not set. There's different logic for DDLs and selects.

template<bool add_default_db, bool add_uuid, bool remove_name>
class RewriteStorageIDVisitor
{
public:
    RewriteStorageIDVisitor(const String & database_name_, std::ostream * ostr_ = nullptr, const Context * context_ = nullptr)
    :   database_name(database_name_),
        visit_depth(0),
        ostr(ostr_),
        context(context_)
    {}

    void visitDDL(ASTPtr & ast) const
    {
        visitDDLChildren(ast);

        if (!tryVisitDynamicCast<ASTQueryWithTableAndOutput>(ast) &&
            !tryVisitDynamicCast<ASTRenameQuery>(ast))
        {}
    }

    void visit(ASTPtr & ast) const
    {
        if (!tryVisit<ASTSelectQuery>(ast) &&
            !tryVisit<ASTSelectWithUnionQuery>(ast) &&
            !tryVisit<ASTFunction>(ast))
            visitChildren(*ast);
    }

    void visit(ASTSelectQuery & select) const
    {
        ASTPtr unused;
        visit(select, unused);
    }

    void visit(ASTSelectWithUnionQuery & select) const
    {
        ASTPtr unused;
        visit(select, unused);
    }

private:
    const String database_name;
    mutable size_t visit_depth;
    std::ostream * ostr;
    const Context * context;

    void visit(ASTSelectWithUnionQuery & select, ASTPtr &) const
    {
        for (auto & child : select.list_of_selects->children)
            tryVisit<ASTSelectQuery>(child);
    }

    void visit(ASTSelectQuery & select, ASTPtr &) const
    {
        if (select.tables())
            tryVisit<ASTTablesInSelectQuery>(select.refTables());

        visitChildren(select);
    }

    void visit(ASTTablesInSelectQuery & tables, ASTPtr &) const
    {
        for (auto & child : tables.children)
            tryVisit<ASTTablesInSelectQueryElement>(child);
    }

    void visit(ASTTablesInSelectQueryElement & tables_element, ASTPtr &) const
    {
        if (tables_element.table_expression)
            tryVisit<ASTTableExpression>(tables_element.table_expression);
    }

    void visit(ASTTableExpression & table_expression, ASTPtr &) const
    {
        if (table_expression.database_and_table_name)
            tryVisit<ASTIdentifier>(table_expression.database_and_table_name);
        else if (table_expression.subquery)
            tryVisit<ASTSubquery>(table_expression.subquery);
    }

    /// @note It expects that only table (not column) identifiers are visited.
    void visit(const ASTIdentifier & identifier, ASTPtr & ast) const
    {
        if constexpr (add_default_db)
        {
            if (!identifier.compound())
            {
                auto uuid = identifier.uuid;
                ast = createTableIdentifier(database_name, identifier.name);
                ast->as<ASTIdentifier &>().uuid = uuid;
            }
        }
        if constexpr (add_uuid)
        {
            auto & id = ast->as<ASTIdentifier &>();
            if (id.uuid.empty())
            {
                DatabaseAndTableWithAlias names(id);
                //TODO easier way to get uuid by db and table names
                id.uuid = context->getTable(names.database, names.table)->getStorageID().uuid;
            }
        }
        if constexpr (remove_name)
        {
            /// Replace table name with placeholder if there is table name
            auto & id = ast->as<ASTIdentifier &>();
            if (!id.uuid.empty())
                id.setShortName(TABLE_WITH_UUID_NAME_PLACEHOLDER);
        }
    }

    void visit(ASTSubquery & subquery, ASTPtr &) const
    {
        tryVisit<ASTSelectWithUnionQuery>(subquery.children[0]);
    }

    void visit(ASTFunction & function, ASTPtr &) const
    {
        bool is_operator_in = false;
        for (auto name : {"in", "notIn", "globalIn", "globalNotIn"})
        {
            if (function.name == name)
            {
                is_operator_in = true;
                break;
            }
        }

        for (auto & child : function.children)
        {
            if (child.get() == function.arguments.get())
            {
                for (size_t i = 0; i < child->children.size(); ++i)
                {
                    if (is_operator_in && i == 1)
                    {
                        /// Second argument of the "in" function (or similar) may be a table name or a subselect.
                        /// Rewrite the table name or descend into subselect.
                        if (!tryVisit<ASTIdentifier>(child->children[i]))
                            visit(child->children[i]);
                    }
                    else
                        visit(child->children[i]);
                }
            }
            else
                visit(child);
        }
    }

    void visitChildren(IAST & ast) const
    {
        for (auto & child : ast.children)
            visit(child);
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast) const
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            DumpASTNode dump(*ast, ostr, visit_depth, "addDefaultDatabaseName");
            visit(*t, ast);
            return true;
        }
        return false;
    }


    void visitDDL(ASTQueryWithTableAndOutput & node, ASTPtr &) const
    {
        if constexpr (add_default_db)
        {
            if (node.database.empty())
                node.database = database_name;
        }
        if constexpr (add_uuid)
        {
            if (auto create = typeid_cast<ASTCreateQuery *>(&node))
            {
                if (create->is_materialized_view && create->to_table_id.uuid.empty())
                    create->to_table_id.uuid = context->getTable(node.database, node.table)->getStorageID().uuid;
            }
            else if (node.uuid.empty()) /// Do not try to get UUID for CREATE query
                    node.uuid = context->getTable(node.database, node.table)->getStorageID().uuid;
        }
        if constexpr (remove_name)
        {
            if (node.uuid.empty())
                return;
            node.database.clear();
            node.table = TABLE_WITH_UUID_NAME_PLACEHOLDER;
            if (auto create = typeid_cast<ASTCreateQuery *>(&node))
            {
                create->to_table_id.database_name.clear();
                create->to_table_id.table_name = TABLE_WITH_UUID_NAME_PLACEHOLDER;
            }
        }
    }

    void visitDDL(ASTRenameQuery & node, ASTPtr &) const
    {
        for (ASTRenameQuery::Element & elem : node.elements)
        {
            if (elem.from.database.empty())
                elem.from.database = database_name;
            if (elem.to.database.empty())
                elem.to.database = database_name;
        }
    }

    void visitDDLChildren(ASTPtr & ast) const
    {
        for (auto & child : ast->children)
            visitDDL(child);
    }

    template <typename T>
    bool tryVisitDynamicCast(ASTPtr & ast) const
    {
        if (T * t = dynamic_cast<T *>(ast.get()))
        {
            visitDDL(*t, ast);
            return true;
        }
        return false;
    }
};

using AddDefaultDatabaseVisitor = RewriteStorageIDVisitor<true, false, false>;
using AddUUIDAndDefaultDatabaseVisitor = RewriteStorageIDVisitor<true, true, false>;
using ReplaceTableNameWithUUIDVisitor = RewriteStorageIDVisitor<false, true, true>;

}
