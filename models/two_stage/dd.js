var data = [
     {name: '杭州富阳中医骨髓炎医院（富阳中医康复医院）', value: 1},
     {name: '杭州市富阳区第一人民医院', value: 49},
     {name: '杭州市富阳中医骨伤医院', value: 7},
     {name: '杭州市富阳区第二人民医院', value: 14},
     {name: '杭州市富阳区中医院', value: 18},
     {name: '杭州市富阳区妇幼保健院', value: 27},
     {name: '杭州市富阳区场口社区卫生服务中心', value: 3},
     {name: '杭州富阳汪木英中医骨伤医院', value: 1},
     {name: '中铁五局富阳医院', value: 4},
     {name: '杭州富阳富春江曜阳老年医院', value: 8},
     {name: '富阳阳光老年病医院', value: 6},
     {name: '杭州富阳瑞丰老年医院', value: 2},
     {name: '杭州富阳东吴医院', value: 9},
     {name: '杭州富阳江南医院', value: 6},
     {name: '杭州富阳城中中西医结合医院有限公司', value: 2},
     {name: '杭州富阳颐乐医院', value: 63},
     {name: '杭州富阳树康康复医院', value: 5},
];

var geoCoordMap = {
    '杭州富阳中医骨髓炎医院（富阳中医康复医院）':[119.9471059,30.07534572],
    '杭州市富阳区第一人民医院': [119.937916,30.07405026],
    '杭州市富阳中医骨伤医院':[119.9255863,30.05885129],
    '杭州市富阳区第二人民医院':[119.7444442,29.97976131],
    '杭州市富阳区中医院':[119.9705537,30.05439153],
    '杭州市富阳区妇幼保健院':[119.9355574,30.05315178],
    '杭州市富阳区场口社区卫生服务中心':[119.884565,	29.91825468],
    '杭州富阳汪木英中医骨伤医院':[119.7461071,29.97800932],
    '中铁五局富阳医院':[119.9722617,30.05942582],
    '杭州富阳富春江曜阳老年医院':[120.0336367,30.08283574],
    '富阳阳光老年病医院':[119.7118619,30.26187815],
    '杭州富阳瑞丰老年医院':[119.9199773,30.06489569],
    '杭州富阳东吴医院':[119.9404409,30.07370166],
    '杭州富阳江南医院':[119.9855086,30.02312266],
    '杭州富阳城中中西医结合医院有限公司':[119.9477389,30.07714846],
    '杭州富阳颐乐医院':[119.9531562,30.05378426],
    '杭州富阳树康康复医院':[119.9415789,30.07513836],
};

var convertData = function (data) {
    var res = [];
    for (var i = 0; i < data.length; i++) {
        var geoCoord = geoCoordMap[data[i].name];
        if (geoCoord) {
            res.push({
                name: data[i].name,
                value: geoCoord.concat(data[i].value)
            });
        }
    }
    return res;
};

option = {
    title: {
        text: '',
        left: 'center'
    },
    tooltip : {
        trigger: 'item'
    },
    bmap: {
        center: [119.95, 30.05],
        zoom: 14,
        roam: true,
        mapStyle: {
            styleJson: [{
                'featureType': 'water',
                'elementType': 'all',
                'stylers': {
                    'color': '#00000'
                }
            }, {
                'featureType': 'land',
                'elementType': 'all',
                'stylers': {
                    'color': '#00000'
                }
            }, {
                'featureType': 'railway',
                'elementType': 'all',
                'stylers': {
                    'visibility': 'off'
                }
            }, {
                'featureType': 'highway',
                'elementType': 'all',
                'stylers': {
                    'color': '#00000'
                }
            }, {
                'featureType': 'highway',
                'elementType': 'labels',
                'stylers': {
                    'visibility': 'off'
                }
            }, {
                'featureType': 'arterial',
                'elementType': 'geometry',
                'stylers': {
                    'color': '#00000'
                }
            }, {
                'featureType': 'arterial',
                'elementType': 'geometry.fill',
                'stylers': {
                    'color': '#00000'
                }
            }, {
                'featureType': 'poi',
                'elementType': 'all',
                'stylers': {
                    'visibility': 'off'
                }
            }, {
                'featureType': 'green',
                'elementType': 'all',
                'stylers': {
                    'visibility': 'off'
                }
            }, {
                'featureType': 'subway',
                'elementType': 'all',
                'stylers': {
                    'visibility': 'off'
                }
            }, {
                'featureType': 'manmade',
                'elementType': 'all',
                'stylers': {
                    'color': '#00000'
                }
            }, {
                'featureType': 'local',
                'elementType': 'all',
                'stylers': {
                    'color': '#00000'
                }
            }, {
                'featureType': 'arterial',
                'elementType': 'labels',
                'stylers': {
                    'visibility': 'off'
                }
            }, {
                'featureType': 'boundary',
                'elementType': 'all',
                'stylers': {
                    'color': '#00000'
                }
            }]
        }
    },
    series : [
        {
            name: '异常人员数量',
            type: 'scatter',
            coordinateSystem: 'bmap',
            data: convertData(data),
            symbolSize: function (val) {
                return val[2]*1.2;
            },
            encode: {
                value: 2
            },
            label: {
                formatter: '{b}',
                position: 'bottom',
                show: false
            },
            itemStyle: {
                color: 'purple'
            },
            emphasis: {
                label: {
                    show: true
                }
            }
        },
    ]
};