import os
import re

file_path = 'src/index/hdr_forest.cpp'

with open(file_path, 'r') as f:
    content = f.read()

# 1. Disable pruning in query_for_join
pruning_pattern = r'(\s*// 剪枝逻辑\s*if \(!tree->center\.empty\(\)\) \{[\s\S]*?continue; // 已剪枝！\s*\}\s*\})'
replacement = r'\n        // 剪枝逻辑 (Disabled)\n        /*\1\n        */'

# Check if already disabled
if '/*\n        // 剪枝逻辑' not in content:
    new_content = re.sub(pruning_pattern, replacement, content)
else:
    new_content = content

# 2. Ensure config parameters
# We want distance_bound_ratio = 4.0f and pca_sample_size = 2000
# We will use regex to find and replace these assignments

# Replace distance_bound_ratio assignment
new_content = re.sub(r'config\.distance_bound_ratio = [\d\.]+f;', 'config.distance_bound_ratio = 4.0f;', new_content)

# Replace pca_sample_size assignment
new_content = re.sub(r'config\.pca_sample_size = \d+;', 'config.pca_sample_size = 2000;', new_content)

if content == new_content:
    print("No changes made.")
else:
    with open(file_path, 'w') as f:
        f.write(new_content)
    print(f"Updated {file_path}")

