#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONCompactEachRowWithProgressRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/JSONUtils.h>

namespace DB
{


JSONCompactEachRowWithProgressRowOutputFormat::JSONCompactEachRowWithProgressRowOutputFormat(WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_,
        bool with_names_,
        bool with_types_,
        bool yield_strings_)
    : RowOutputFormatWithUTF8ValidationAdaptor(settings_.json.validate_utf8, header_, out_, params_)
    , settings(settings_)
    , with_names(with_names_)
    , with_types(with_types_)
    , yield_strings(yield_strings_)
{
}


void JSONCompactEachRowWithProgressRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    if (yield_strings)
    {
        WriteBufferFromOwnString buf;

        serialization.serializeText(column, row_num, buf, settings);
        writeJSONString(buf.str(), *ostr, settings);
    }
    else
        serialization.serializeTextJSON(column, row_num, *ostr, settings);
}


void JSONCompactEachRowWithProgressRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", *ostr);
}


void JSONCompactEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    if (has_progress)
        writeProgress();
    writeChar('[', *ostr);
}


void JSONCompactEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("]\n", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
    String progress_line;
    WriteBufferFromString buf(progress_line);
    writeChar('[', buf);
    progress.writeJSONCompact(buf);
    writeCString("]\n", buf);
    buf.finalize();
    std::lock_guard lock(progress_lines_mutex);
    progress_lines.emplace_back(std::move(progress_line));
    has_progress = true;
}

void JSONCompactEachRowWithProgressRowOutputFormat::flush()
{
    if (has_progress)
        writeProgress();
    JSONCompactEachRowWithProgressRowOutputFormat::flush();
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeChar('\n', *ostr);
    size_t columns_size = columns.size();
    writeRowStartDelimiter();
    for (size_t i = 0; i < columns_size; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }
    writeRowEndDelimiter();
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeLine(const std::vector<String> & values)
{
    JSONUtils::makeNamesValidJSONStrings(values, settings, settings.json.validate_utf8);
    writeRowStartDelimiter();
    for (size_t i = 0; i < values.size(); ++i)
    {
        writeChar('\"', *ostr);
        writeString(values[i], *ostr);
        writeChar('\"', *ostr);
        if (i != values.size() - 1)
            writeFieldDelimiter();
    }
    writeRowEndDelimiter();
}

void JSONCompactEachRowWithProgressRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();

    if (with_names)
        writeLine(JSONUtils::makeNamesValidJSONStrings(header.getNames(), settings, settings.json.validate_utf8));

    if (with_types)
        writeLine(JSONUtils::makeNamesValidJSONStrings(header.getDataTypeNames(), settings, settings.json.validate_utf8));
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeSuffix()
{
    if (has_progress)
        writeProgress();
    JSONCompactEachRowWithProgressRowOutputFormat::writeSuffix();
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeProgress()
{
    std::lock_guard lock(progress_lines_mutex);
    for (const auto & progress_line : progress_lines)
        writeString(progress_line,  *ostr);
    progress_lines.clear();
    has_progress = false;
}

void JSONCompactEachRowWithProgressRowOutputFormat::consumeTotals(DB::Chunk chunk)
{
    if (with_names)
        IRowOutputFormat::consumeTotals(std::move(chunk));
}

void registerOutputFormatJSONCompactEachRowWithProgress(FormatFactory & factory)
{
    for (bool yield_strings : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerOutputFormat(format_name, [yield_strings, with_names, with_types](
                WriteBuffer & buf,
                const Block & sample,
                const RowOutputFormatParams & params,
                const FormatSettings & format_settings)
            {
                return std::make_shared<JSONCompactEachRowWithProgressRowOutputFormat>(buf, sample, params, format_settings, with_names, with_types, yield_strings);
            });

            factory.markOutputFormatSupportsParallelFormatting(format_name);
        };

        registerWithNamesAndTypes(yield_strings ? "JSONCompactStringsEachRowWithProgress" : "JSONCompactEachRowWithProgress", register_func);
    }
}


}
