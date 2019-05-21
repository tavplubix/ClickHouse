#pragma once

#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Formats/FormatSettings.h>
#include <Parsers/TokenIterator.h>

namespace DB
{

class ConstantExpressionTemplate
{
public:
    ConstantExpressionTemplate(const IDataType & result_column_type, TokenIterator begin, TokenIterator end, const Context & context);

    void parseExpression(ReadBuffer & istr, const FormatSettings & settings);

    ColumnPtr evaluateAll();

private:
    std::pair<String, NamesAndTypes> replaceLiteralsWithDummyIdentifiers(TokenIterator & begin, TokenIterator & end);

    static void addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr);

    // TODO void fixArgumentsThatAreAlwaysConstant(...);
    void fixUnarySignsAndIntegralTypes(NamesAndTypes & columns_for_literals, ASTPtr & ast_template);
    void fixNullable(NamesAndTypes & columns_for_literals, bool result_type_is_nullable);

private:
    std::vector<String> tokens;
    std::vector<size_t> token_after_literal_idx;
    String result_column_name;
    ExpressionActionsPtr actions_on_literals;
    Block literals;
    MutableColumns columns;

    /// For expressions without literals (e.g. "now()")
    size_t rows_count = 0;

};

}
