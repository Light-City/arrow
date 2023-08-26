#pragma once

#include <vector>

#include "arrow/acero/options.h"
#include "arrow/acero/schema_util.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {

using compute::ExecContext;

namespace acero {

class ARROW_EXPORT NestedLoopJoinSchema {
 public:
  Status Init(JoinType join_type, const Schema& left_schema, const Schema& right_schema,
              const Expression& filter, const std::string& left_field_name_prefix,
              const std::string& right_field_name_prefix);

  Status Init(JoinType join_type, const Schema& left_schema,
              const std::vector<FieldRef>& left_output, const Schema& right_schema,
              const std::vector<FieldRef>& right_output, const Expression& filter,
              const std::string& left_field_name_prefix,
              const std::string& right_field_name_prefix);

  static Status ValidateSchemas(JoinType join_type, const Schema& left_schema,
                                const std::vector<FieldRef>& left_output,
                                const Schema& right_schema,
                                const std::vector<FieldRef>& right_output);

  std::shared_ptr<Schema> MakeOutputSchema(const std::string& left_field_name_suffix,
                                           const std::string& right_field_name_suffix);

  SchemaProjectionMaps<NestedLoopJoinProjection> proj_maps[2];

  Result<Expression> BindFilter(Expression filter, const Schema& left_schema,
                                const Schema& right_schema, ExecContext* exec_context);

 private:
  Status CollectFilterColumns(std::vector<FieldRef>& left_filter,
                              std::vector<FieldRef>& right_filter,
                              const Expression& filter, const Schema& left_schema,
                              const Schema& right_schema);
};

}  // namespace acero
}  // namespace arrow