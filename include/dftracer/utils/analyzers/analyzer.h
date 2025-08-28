#ifndef DFTRACER_UTILS_ANALYZERS_ANALYZER_H
#define DFTRACER_UTILS_ANALYZERS_ANALYZER_H

#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/analyzers/trace.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/utils/json.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/analyzers/configuration_manager.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace dftracer::utils::analyzers {

class Analyzer {
   public:
    Analyzer(const AnalyzerConfigManager& config);

    Analyzer(const Analyzer&) = delete;
    Analyzer& operator=(const Analyzer&) = delete;
    Analyzer(Analyzer&&) = default;
    Analyzer& operator=(Analyzer&&) = default;
    ~Analyzer() = default;

    inline const AnalyzerConfigManager& config() const { return config_; }

    Pipeline analyze(const std::vector<std::string>& traces,
        const std::vector<std::string>& view_types,
        const std::vector<std::string>& exclude_characteristics = {},
        const std::unordered_map<std::string, std::string>& extra_columns = {}
        // @todo: add extra_columns_fn
    );

   protected:
    std::string get_checkpoint_name(const std::vector<std::string>& args) const;
    std::string get_checkpoint_path(const std::string& name) const;
    bool has_checkpoint(const std::string& name) const;
    Pipeline compute_high_level_metrics(
        const std::vector<Trace>& trace_records, const std::vector<std::string>& view_types);

   private:
    AnalyzerConfigManager config_;
};

}  // namespace dftracer::utils::analyzers

#endif  // DFTRACER_UTILS_ANALYZERS_ANALYZER_H
