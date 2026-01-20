#!/bin/bash

# 配置项
TEST_NAME="3D" # 或者直接填 "3B" 运行所有 3B 测试
COUNT=20
PARALLEL_JOBS=32 # 并行执行的任务数，建议设置为 CPU 核心数
PASS_COUNT=0
FAIL_COUNT=0

echo "开始执行 $COUNT 次 $TEST_NAME 测试..."

for ((i=1; i<=$COUNT; i++))
do
    # 使用 -race 检查竞争，输出重定向到临时文件
    # 注意修改为你实际的路径，例如 ./src/raft
    go test -run $TEST_NAME -race > tmp_test.log 2>&1
    
    if [ $? -eq 0 ]; then
        ((PASS_COUNT++))
        echo "测试 $i: [PASS]"
    else
        ((FAIL_COUNT++))
        echo "测试 $i: [FAIL] - 详情见 error_$i.log"
        mv tmp_test.log "error_$i.log"
    fi
done

echo "--------------------------------"
echo "测试完成！"
echo "成功: $PASS_COUNT"
echo "失败: $FAIL_COUNT"

if [ $FAIL_COUNT -gt 0 ]; then
    echo "请检查生成的 error_*.log 文件获取详细报错信息。"
fi