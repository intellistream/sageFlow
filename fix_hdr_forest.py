import sys

file_path = 'src/index/hdr_forest.cpp'

with open(file_path, 'r') as f:
    lines = f.readlines()

new_lines = []
skip = False
for i, line in enumerate(lines):
    if 'else {' in line and '// 回退' in lines[i+1]:
        new_lines.append(line)
        new_lines.append(lines[i+1])
        new_lines.append('            for (auto uid : tree->user_ids) {\n')
        new_lines.append('                if (storage_manager_ && storage_manager_->engine_) {\n')
        new_lines.append('                    auto rec = storage_manager_->getVectorByUid(uid);\n')
        new_lines.append('                    if (rec) {\n')
        new_lines.append('                        float sim = storage_manager_->engine_->Similarity(record.data_, rec->data_);\n')
        new_lines.append('                        if (sim >= join_similarity_threshold) {\n')
        new_lines.append('                            results.push_back(uid);\n')
        new_lines.append('                        }\n')
        new_lines.append('                    }\n')
        new_lines.append('                } else {\n')
        new_lines.append('                    results.push_back(uid);\n')
        new_lines.append('                }\n')
        new_lines.append('            }\n')
        new_lines.append('        }\n')
        # Skip original lines
        # Original was:
        # } else {
        #     // 回退
        #     for (auto uid : tree->user_ids) {
        #         results.push_back(uid);
        #     }
        # }
        # We appended the first two lines, so we need to skip the next 4 lines of the original file
        # But wait, I am iterating.
        # Let's just replace the block.
        pass
    else:
        # This logic is getting complicated. Let's use string replacement on the whole content.
        pass

with open(file_path, 'r') as f:
    content = f.read()

old_block_1 = """        } else {
            // 回退
            for (auto uid : tree->user_ids) {
                results.push_back(uid);
            }
        }"""

new_block_1 = """        } else {
            // 回退
            for (auto uid : tree->user_ids) {
                if (storage_manager_ && storage_manager_->engine_) {
                    auto rec = storage_manager_->getVectorByUid(uid);
                    if (rec) {
                        float sim = storage_manager_->engine_->Similarity(record.data_, rec->data_);
                        if (sim >= join_similarity_threshold) {
                            results.push_back(uid);
                        }
                    }
                } else {
                    results.push_back(uid);
                }
            }
        }"""

old_block_2 = """    for(auto uid : insert_buffer_) {
        results.push_back(uid);
    }"""

new_block_2 = """    for(auto uid : insert_buffer_) {
        if (storage_manager_ && storage_manager_->engine_) {
            auto rec = storage_manager_->getVectorByUid(uid);
            if (rec) {
                float sim = storage_manager_->engine_->Similarity(record.data_, rec->data_);
                if (sim >= join_similarity_threshold) {
                    results.push_back(uid);
                }
            }
        } else {
            results.push_back(uid);
        }
    }"""

if old_block_1 in content:
    content = content.replace(old_block_1, new_block_1)
    print("Replaced block 1")
else:
    print("Block 1 not found")

if old_block_2 in content:
    content = content.replace(old_block_2, new_block_2)
    print("Replaced block 2")
else:
    print("Block 2 not found")

with open(file_path, 'w') as f:
    f.write(content)

