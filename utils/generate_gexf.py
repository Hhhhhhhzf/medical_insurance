# from gexf import Gexf
import os

MOVE_DISEASE = ['疾病名称不详', '原发性高血压', '特发性(原发性)高血压', '预防措施', '健康查体', '责任医生签约']
MOVE_DISEASE = set(MOVE_DISEASE)


def read_relation_simple(path, file_name):
    outputs = []
    with open(os.path.join(path, file_name), 'r') as f:
        for line in f.readlines():
            if line != '':
                line = line[:-1]
                line = eval(line)
                if len(line[0]) == 2:
                    d1, d2, w = line[0][0], line[0][1], line[1]
                    if d1 in MOVE_DISEASE or d2 in MOVE_DISEASE:
                        continue
                    if '高血压' in d1:
                        d1 = '高血压'
                    if '高血压' in d2:
                        d2 = '高血压'
                    if d1 != d2:
                        outputs.append([d1, d2, w])
                else:
                    d1, d2, d3, w = line[0][0], line[0][1], line[0][2], line[1]
                    if d1 in MOVE_DISEASE or d2 in MOVE_DISEASE or d3 in MOVE_DISEASE:
                        continue
                    if '高血压' in d1:
                        d1 = '高血压'
                    if '高血压' in d2:
                        d2 = '高血压'
                    if '高血压' in d3:
                        d3 = '高血压'
                    if d1 != d2:
                        outputs.append([d1, d2, w])
                    if d1 != d3:
                        outputs.append([d1, d3, w])
                    if d2 != d3:
                        outputs.append([d2, d3, w])
    return outputs


def read_relation(path, file_name):
    sum_weight = idx = 0
    nodes, edges, id2label, label2id = [], [], {}, {}
    id2weight = {}
    with open(os.path.join(path, file_name), 'r') as f:
        for line in f.readlines():
            line = eval(line)
            d1, d2, w = line[0][0], line[0][1], line[1]
            if d1 in MOVE_DISEASE or d2 in MOVE_DISEASE:
                continue
            sum_weight += w
            if d1 not in label2id:
                label2id[d1] = idx
                id2label[idx] = d1
                id2weight[idx] = 0
                nodes.append((idx, d1))
                idx += 1
            if d2 not in label2id:
                label2id[d2] = idx
                id2label[idx] = d2
                id2weight[idx] = 0
                nodes.append((idx, d2))
                idx += 1
    with open(os.path.join(path, file_name), 'r') as f:
        for line in f.readlines():
            line = eval(line)
            d1, d2, w = line[0][0], line[0][1], line[1]
            id1, id2 = label2id[d1], label2id[d2]
            id2weight[id1] += w/sum_weight
            id2weight[id2] += w/sum_weight
            edges.append((id1, id2))
    return nodes, edges, id2label, label2id, id2weight


# def build_graph():
#     gexf = Gexf("xxx", "yyy")
#     graph = gexf.addGraph("undirected", "static", "xxx")
#
#     # 为图设置节点的属性
#
#     ########注意添加catalogy与需要使用到的属性名一致的force_id
#     attr_mod_network = graph.addNodeAttribute(force_id='modularity_class', title='Modularity Class', type='integer')
#
#     ##添加节点
#     def add_nodes(sheet, net):
#         node_id = your_id
#         temp_name = your_name
#         catagory = your_catagory
#
#     ##id, name, network
#     tmp_node = graph.addNode(str(node_id), temp_name)
#     tmp_node.addAttribute(attr_mod_network, your_catagory)
#
#
#     ##添加边
#     def add_edges(sheet):
#         src_id = your_src_id
#         dst_id = your_dst_id
#         weight = 1
#         ##注意：count_index要是唯一的
#         graph.addEdge(count_index, src_id, dst_id, weight=weight)
#
#
#     #############以上就是基本的操作了
#     #######如果要为node添加一些viz的元素，通过xml元素的角度进行下手
#     '''
#             <viz:size value="28.685715"></viz:size>
#             <viz:position x="-266.82776" y="299.6904" z="0.0"></viz:position>
#             <viz:color r="235" g="81" b="72"></viz:color>
#     '''
#     from lxml import etree
#
#     for gexf_elem in gexf_xml:
#         if gexf_elem.tag == 'graph':
#             for gexf_nodes_links in gexf_elem:
#                 if gexf_nodes_links.tag == 'nodes':
#                     for node in gexf_nodes_links:
#                         tmp_id = node.get('id')
#                         node_id = tmp_id
#                         for attrs in node:
#                             for attr in attrs:
#                                 ##为不同类目设置不同viz
#                                 if attr.attrib['for'] == "modularity_class":
#                                     size_value = your_size_value
#                                     size = etree.SubElement(node, '{%s}size' % gexf.viz)
#                                     size.set('value', size_value)
#
#                                     pos_x = your_x
#                                     pos_y = your_y
#                                     pos_z = your_z
#                                     position = etree.SubElement(node, '{%s}position' % gexf.viz)
#                                     position.set('x', str(pos_x))
#                                     position.set('y', str(pos_y))
#                                     position.set('z', str(pos_z))
#
#                                     color = etree.SubElement(node, '{%s}color' % gexf.viz)
#                                     color.set('r', node_rgb[0])
#                                     color.set('g', node_rgb[1])
#                                     color.set('b', node_rgb[2])
#
#                 elif gexf_nodes_links.tag == 'edges':
#                     print("--------------dealing with edges viz--------------")
#                     for edge in gexf_nodes_links:
#                         weight = your_weight
#                         thickness = etree.SubElement(edge, '{%s}thickness' % gexf.viz)
#                         thickness.set('value', weight)
#
#     out_file_name = your_out_file_name
#
#     output_file = open(out_file_name, "wb")
#     output_file.write(etree.tostring(gexf_xml, pretty_print=True, encoding='utf-8', xml_declaration=True))


if __name__ == '__main__':
    path, file = '../data/two_stage/data/', 'package_disease_ans_all_0.02.txt'
    print(read_relation_simple(path, file))

