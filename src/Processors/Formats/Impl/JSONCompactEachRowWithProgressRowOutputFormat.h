#pragma once
#include <mutex>
#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** The stream for outputting data in JSON format, by object per line.
  */
class JSONCompactEachRowWithProgressRowOutputFormat final : public RowOutputFormatWithUTF8ValidationAdaptor
{
public:
    JSONCompactEachRowWithProgressRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_,
        bool with_names_,
        bool with_types_,
        bool yield_strings_);

    String getName() const override { return "JSONCompactEachRowRowWithProgressRowOutputFormat"; }
    void onProgress(const Progress & value) override;
    void flush() override;    

private:
    void writePrefix() override;

    void writeTotals(const Columns & columns, size_t row_num) override;

    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeSuffix() override;
    void writeProgress();
    bool supportTotals() const override { return true; }
    void consumeTotals(Chunk) override;

    void writeLine(const std::vector<String> & values);

    FormatSettings settings;
    bool with_names;
    bool with_types;
    bool yield_strings;

    Progress progress;
    std::vector<String> progress_lines;
    std::mutex progress_lines_mutex;
    std::atomic_bool has_progress = false;
};
}
