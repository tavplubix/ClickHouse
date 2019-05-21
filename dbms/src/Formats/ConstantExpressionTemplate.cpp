
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Formats/ConstantExpressionTemplate.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/FieldToDataType.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_EXPRESSION_TEMPLATE;
    extern const int CANNOT_PARSE_EXPRESSION_USING_TEMPLATE;
    extern const int CANNOT_EVALUATE_EXPRESSION_TEMPLATE;
}



class SignBeforeNumericLiteralsMatcher
{
public:
    using Visitor = InDepthNodeVisitor<SignBeforeNumericLiteralsMatcher, true>;

    struct Data
    {
        explicit Data(const NamesAndTypes & columns_for_literals_)
            : columns_for_literals(columns_for_literals_),
              has_unary_minus(columns_for_literals.size(), false),
              has_binary_plus(columns_for_literals.size(), false)
        {
            for (size_t i = 0; i < columns_for_literals.size(); ++i)
                name_to_idx[columns_for_literals[i].name] = i;
        }

        const NamesAndTypes & columns_for_literals;
        std::vector<char> has_unary_minus;
        std::vector<char> has_binary_plus;
        std::map<String, size_t> name_to_idx;
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        auto function = ast->as<ASTFunction>();
        if (!function) return;
        auto args = function->arguments->as<ASTExpressionList>();
        if (!args) return;
        if (function->name == "negate")
        {
            auto col = args->children[0]->as<ASTIdentifier>();
            if (!col) return;
            auto idx = data.name_to_idx.find(col->name);
            if (idx == data.name_to_idx.end()) return;
            data.has_unary_minus[idx->second] = true;
            function->name = "identity";
        }
        else if (function->name == "plus")
        {
            auto col = args->children[1]->as<ASTIdentifier>();
            if (!col) return;
            auto idx = data.name_to_idx.find(col->name);
            if (idx == data.name_to_idx.end()) return;
            data.has_binary_plus[idx->second] = true;
        }
    }
    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }
};

using SignBeforeNumericLiteralsVisitor = SignBeforeNumericLiteralsMatcher::Visitor;

ConstantExpressionTemplate::ConstantExpressionTemplate(const IDataType & result_column_type, TokenIterator begin, TokenIterator end,
                                                       const Context & context)
{
    std::pair<String, NamesAndTypes> expr_template = replaceLiteralsWithDummyIdentifiers(begin, end);


    std::cerr << "\n==========================\nReplacing expression with template `" << expr_template.first << "`\n";

    ParserExpression parser;
    Expected expected;
    Tokens template_tokens(expr_template.first.data(), expr_template.first.data() + expr_template.first.size());
    TokenIterator token_iterator1(template_tokens);

    ASTPtr ast_template;
    if (!parser.parse(token_iterator1, ast_template, expected))
        throw Exception("Cannot parse template after replacing literals: " + expr_template.first, ErrorCodes::CANNOT_CREATE_EXPRESSION_TEMPLATE);

    fixUnarySignsAndIntegralTypes(expr_template.second, ast_template);
    fixNullable(expr_template.second, result_column_type.isNullable());

    for (const auto & col : expr_template.second)
        literals.insert({nullptr, col.type, col.name});
    columns = literals.cloneEmptyColumns();

    addNodesToCastResult(result_column_type, ast_template);
    result_column_name = ast_template->getColumnName();

    std::cerr << literals.dumpStructure() << "\n";
    ast_template->dumpTree(std::cerr, 10);
    std::cerr << "\n\n";

    // TODO convert SyntaxAnalyzer and ExpressionAnalyzer exceptions to CANNOT_CREATE_EXPRESSION_TEMPLATE
    auto syntax_result = SyntaxAnalyzer(context).analyze(ast_template, NamesAndTypesList(expr_template.second.begin(), expr_template.second.end()));

    actions_on_literals = ExpressionAnalyzer(ast_template, syntax_result, context).getActions(false);

    std::cerr << actions_on_literals->dumpActions() << "\n==============================\n\n";
}

void ConstantExpressionTemplate::parseExpression(ReadBuffer & istr, const FormatSettings & settings)
{
    size_t cur_column = 0;
    try
    {
        size_t cur_token = 0;
        while (cur_column < literals.columns())
        {
            size_t skip_tokens_until = token_after_literal_idx[cur_column];
            while (cur_token < skip_tokens_until)
            {
                // TODO skip comments
                skipWhitespaceIfAny(istr);
                assertString(tokens[cur_token++], istr);
            }
            skipWhitespaceIfAny(istr);
            const IDataType & type = *literals.getByPosition(cur_column).type;
            type.deserializeAsTextQuoted(*columns[cur_column], istr, settings);
            ++cur_column;
        }
        while (cur_token < tokens.size())
        {
            skipWhitespaceIfAny(istr);
            assertString(tokens[cur_token++], istr);
        }
        ++rows_count;
    }
    catch (DB::Exception & e)
    {
        for (size_t i = 0; i < cur_column; ++i)
            columns[i]->popBack(1);

        if (!isParseError(e.code()))
            throw;

        throw DB::Exception("Cannot parse expression using template", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
    }
}

ColumnPtr ConstantExpressionTemplate::evaluateAll()
{
    Block evaluated = literals.cloneWithColumns(std::move(columns));
    columns = literals.cloneEmptyColumns();
    if (!literals.columns())
        evaluated.insert({ColumnConst::create(ColumnUInt8::create(1, 0), rows_count), std::make_shared<DataTypeUInt8>(), "_dummy"});
    actions_on_literals->execute(evaluated);

    if (!evaluated || evaluated.rows() == 0)
        throw Exception("Logical error: empty block after evaluation of batch of constant expressions",
                        ErrorCodes::LOGICAL_ERROR);

    if (!evaluated.has(result_column_name))
        throw Exception("Cannot evaluate template " + result_column_name + ", block structure:\n" + evaluated.dumpStructure(),
                        ErrorCodes::CANNOT_EVALUATE_EXPRESSION_TEMPLATE);


    return evaluated.getByName(result_column_name).column->convertToFullColumnIfConst();
}

std::pair<String, NamesAndTypes>
ConstantExpressionTemplate::replaceLiteralsWithDummyIdentifiers(TokenIterator & begin, TokenIterator & end)
{
    NamesAndTypes dummy_columns;
    String result;
    size_t token_idx = 0;
    while (begin != end)
    {
        const Token & t = *begin;
        if (t.isError())
            throw DB::Exception("Error in tokens", ErrorCodes::CANNOT_CREATE_EXPRESSION_TEMPLATE);

        Expected expected;
        ASTPtr ast;
        DataTypePtr type;

        // TODO don't convert constant arguments of functions such as CAST(x, 'type')

        if (t.type == TokenType::Number)     // TODO Are nan and inf a bare word?
        {
            ParserNumber parser;
            if (!parser.parse(begin, ast, expected))
                throw DB::Exception("Cannot determine literal type: " + String(t.begin, t.size()), ErrorCodes::CANNOT_CREATE_EXPRESSION_TEMPLATE);
            Field & value = ast->as<ASTLiteral &>().value;
            // TODO parse numbers more carefully: distinguish unary and binary sign
            type = DataTypeFactory::instance().get(value.getTypeName());
        }
        else if (t.type == TokenType::StringLiteral)
        {
            type = std::make_shared<DataTypeString>();
            ++begin;
        }
        else if (t.type == TokenType::OpeningSquareBracket)
        {
            ParserArrayOfLiterals parser;
            if (parser.parse(begin, ast, expected))
            {
                Field & value = ast->as<ASTLiteral &>().value;
                type = applyVisitor(FieldToDataType(), value);
            }
        }

        if (type)
        {
            // TODO ensure dummy_col_name is unique (there was no _dummy_x identifier in expression)
            String dummy_col_name = "_dummy_" + std::to_string(dummy_columns.size());

            dummy_columns.push_back(NameAndTypePair(dummy_col_name, type));
            token_after_literal_idx.push_back(token_idx);
            result.append(dummy_col_name);
        }
        else
        {
            tokens.emplace_back(t.begin, t.size());
            result.append(tokens.back());
            ++begin;
            ++token_idx;
        }
        result.append(" ");
    }
    return std::make_pair(result, dummy_columns);
}

void ConstantExpressionTemplate::addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr)
{
    auto result_type = std::make_shared<ASTLiteral>(result_column_type.getName());

    auto arguments = std::make_shared<ASTExpressionList>();
    arguments->children.push_back(std::move(expr));
    arguments->children.push_back(std::move(result_type));

    auto cast = std::make_shared<ASTFunction>();
    cast->name = "CAST";
    cast->arguments = std::move(arguments);
    cast->children.push_back(cast->arguments);

    expr = std::move(cast);
}

void ConstantExpressionTemplate::fixUnarySignsAndIntegralTypes(NamesAndTypes & columns_for_literals, ASTPtr & ast_template)
{
    // TODO mark signs before numbers while replacing literals and do this transformations only if neccessary
    SignBeforeNumericLiteralsVisitor::Data data{columns_for_literals};
    SignBeforeNumericLiteralsVisitor(data).visit(ast_template);

    for (size_t i = 0; i < columns_for_literals.size(); ++i)
    {
        if (data.has_unary_minus[i])
        {
            DataTypePtr & type = columns_for_literals[i].type;
            if (WhichDataType(type).isUInt64())
            {
                // TODO check value fits into Int64, otherwise convert to Float64
                type = std::make_shared<DataTypeInt64>();
            }
        }

        /// Remove unary minus or plus before literal if any
        size_t token_before_literal = token_after_literal_idx[i];
        if (!token_before_literal)
            continue;
        --token_before_literal;
        bool has_sign = tokens[token_before_literal] == "-" || tokens[token_before_literal] == "+";
        bool is_unary_sign = data.has_unary_minus[i] || !data.has_binary_plus[i];
        if (has_sign && is_unary_sign)
        {
            tokens.erase(tokens.begin() + token_before_literal);
            for (size_t j = i; j < token_after_literal_idx.size(); ++j)
                --token_after_literal_idx[j];
        }
    }

}

void ConstantExpressionTemplate::fixNullable(NamesAndTypes & columns_for_literals, bool result_type_is_nullable)
{
    /// Allow literal to be NULL, if result column has nullable type
    if (!result_type_is_nullable)
        return;

    // TODO also allow NULL literals inside functions, which return not NULL for NULL arguments,
    //  even if result_column_type is not nullable
    for (auto & col : columns_for_literals)
        col.type = makeNullable(col.type);
}

}
