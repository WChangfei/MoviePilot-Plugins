import datetime
import re
import xml.dom.minidom
from threading import Event
from typing import Tuple, List, Dict, Any

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfo
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaType
from app.utils.dom import DomUtils
from app.utils.http import RequestUtils


class DoubanRankV2(_PluginBase):
    # 插件名称
    plugin_name = "豆瓣榜单订阅V2"
    # 插件描述
    plugin_desc = "监控豆瓣热门榜单，自动添加订阅。修复官方无法识别媒体信息的问题"
    # 插件图标
    plugin_icon = "movie.jpg"
    # 插件版本
    plugin_version = "1.0.2"
    # 插件作者
    plugin_author = "WChangFei"
    # 作者主页
    author_url = "https://github.com/WChangFei"
    # 插件配置项ID前缀
    plugin_config_prefix = "doubanrankv2_"
    # 加载顺序
    plugin_order = 6
    # 可使用的用户级别
    auth_level = 2

    # 退出事件
    _event = Event()
    # 私有属性
    _scheduler = None
    _douban_address = {
        "mv-weekly": "/douban/movie/weekly",  # 口碑电影榜
        "mv-real-time": "/douban/movie/weekly/movie_real_time_hotest",  # 实时热门电影
        "mv-hot-gaia": "/douban/movie/weekly/movie_hot_gaia",  # 热播电影
        "show": "/douban/movie/weekly/show_domestic",  # 热门综艺
        "tv-hot": "/douban/movie/weekly/tv_hot",  # 近期热门
        "tv-kr-hot": "/douban/list/14786",  # 热播韩剧
        "tv-cn-hot": "/douban/list/EC74443FY",  # 近期热播国产剧
        "tv-hk-top": "/douban/list/ECVM47WUA",  # 高分港剧
        "tv-tw-top": "/douban/list/ECBI5EL6A",  # 高分台剧
        "tv-cn-top": "/douban/list/ECT45KVZI",  # 高分国产剧
        "tv-kr-top": "/douban/list/EC6EC5GBQ",  # 高分韩剧
    }
    _enabled = False
    _cron = ""
    _onlyonce = False
    _rss_addrs = []
    _ranks = []
    _vote = 0
    _min_year = 0
    _title_blacklist = []
    _clear = False
    _clearflag = False
    _proxy = False
    _rsshub = "http://127.0.0.1:1200"

    def init_plugin(self, config: dict = None):

        if config:
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._proxy = config.get("proxy")
            self._onlyonce = config.get("onlyonce")
            self._vote = float(config.get("vote")) if config.get("vote") else 0
            self._min_year = (
                int(config.get("min_year")) if config.get("min_year") else 0
            )
            self._rsshub = config.get("rsshub") or "http://127.0.0.1:1200"
            rss_addrs = config.get("rss_addrs")
            if rss_addrs:
                if isinstance(rss_addrs, str):
                    self._rss_addrs = rss_addrs.split("\n")
                else:
                    self._rss_addrs = rss_addrs
            else:
                self._rss_addrs = []
            self._ranks = config.get("ranks") or []
            # 加载标题黑名单
            title_blacklist_str = config.get("title_blacklist")
            if title_blacklist_str:
                if isinstance(title_blacklist_str, str):
                    self._title_blacklist = [
                        keyword.strip()
                        for keyword in title_blacklist_str.split(",")
                        if keyword.strip()
                    ]
                else:
                    self._title_blacklist = title_blacklist_str
            else:
                self._title_blacklist = []
            self._clear = config.get("clear")

        # 停止现有任务
        self.stop_service()

        # 启动服务
        if self._enabled or self._onlyonce:
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info("豆瓣榜单订阅服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self.__refresh_rss,
                    trigger="date",
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                    + datetime.timedelta(seconds=3),
                )

                if self._scheduler.get_jobs():
                    # 启动服务
                    self._scheduler.print_jobs()
                    self._scheduler.start()

            if self._onlyonce or self._clear:
                # 关闭一次性开关
                self._onlyonce = False
                # 记录缓存清理标志
                self._clearflag = self._clear
                # 关闭清理缓存
                self._clear = False
                # 保存配置
                self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        [{
            "path": "/xx",
            "endpoint": self.xxx,
            "methods": ["GET", "POST"],
            "summary": "API说明"
        }]
        """
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除豆瓣榜单订阅历史记录",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self._enabled and self._cron:
            return [
                {
                    "id": "DoubanRankV2",
                    "name": "豆瓣榜单订阅服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.__refresh_rss,
                    "kwargs": {},
                }
            ]
        elif self._enabled:
            return [
                {
                    "id": "DoubanRank",
                    "name": "豆瓣榜单订阅服务",
                    "trigger": CronTrigger.from_crontab("0 8 * * *"),
                    "func": self.__refresh_rss,
                    "kwargs": {},
                }
            ]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "proxy",
                                            "label": "使用代理服务器",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "5位cron表达式，留空自动",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "vote",
                                            "label": "评分",
                                            "placeholder": "评分大于等于该值才订阅",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "min_year",
                                            "label": "最小发布年份",
                                            "placeholder": "0表示不限制",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "rsshub",
                                            "label": "RSSHub地址",
                                            "placeholder": "http://127.0.0.1:1200",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "chips": True,
                                            "multiple": True,
                                            "model": "ranks",
                                            "label": "热门榜单",
                                            "items": [
                                                {
                                                    "title": "口碑电影榜",
                                                    "value": "mv-weekly",
                                                },
                                                {
                                                    "title": "实时热门电影",
                                                    "value": "mv-real-time",
                                                },
                                                {
                                                    "title": "热播电影",
                                                    "value": "mv-hot-gaia",
                                                },
                                                {"title": "热门综艺", "value": "show"},
                                                {
                                                    "title": "近期热门",
                                                    "value": "tv-hot",
                                                },
                                                {
                                                    "title": "热播韩剧",
                                                    "value": "tv-kr-hot",
                                                },
                                                {
                                                    "title": "近期热播国产剧",
                                                    "value": "tv-cn-hot",
                                                },
                                                {
                                                    "title": "高分港剧",
                                                    "value": "tv-hk-top",
                                                },
                                                {
                                                    "title": "高分台剧",
                                                    "value": "tv-tw-top",
                                                },
                                                {
                                                    "title": "高分国产剧",
                                                    "value": "tv-cn-top",
                                                },
                                                {
                                                    "title": "高分韩剧",
                                                    "value": "tv-kr-top",
                                                },
                                            ],
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "rss_addrs",
                                            "label": "自定义榜单地址",
                                            "placeholder": "每行一个地址，如：http://127.0.0.1:1200/douban/movie/ustop",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "title_blacklist",
                                            "label": "标题黑名单",
                                            "placeholder": "多个关键字用逗号分隔，如：柯南,海贼王,火影",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clear",
                                            "label": "清理历史记录",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "cron": "",
            "proxy": False,
            "onlyonce": False,
            "vote": "",
            "min_year": "",
            "rsshub": "http://127.0.0.1:1200",
            "ranks": [],
            "rss_addrs": "",
            "title_blacklist": "",
            "clear": False,
        }

    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面，需要返回页面配置，同时附带数据
        """
        # 查询历史记录
        historys = self.get_data("history")
        if not historys:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {
                        "class": "text-center",
                    },
                }
            ]
        # 数据按时间降序排序
        historys = sorted(historys, key=lambda x: x.get("time"), reverse=True)
        # 拼装页面
        contents = []
        for history in historys:
            title = history.get("title")
            poster = history.get("poster")
            mtype = history.get("type")
            time_str = history.get("time")
            doubanid = history.get("doubanid")
            contents.append(
                {
                    "component": "VCard",
                    "content": [
                        {
                            "component": "VDialogCloseBtn",
                            "props": {
                                "innerClass": "absolute top-0 right-0",
                            },
                            "events": {
                                "click": {
                                    "api": "plugin/DoubanRankV2/delete_history",
                                    "method": "get",
                                    "params": {
                                        "key": f"doubanrank: {title} (DB:{doubanid})",
                                        "apikey": settings.API_TOKEN,
                                    },
                                }
                            },
                        },
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex justify-space-start flex-nowrap flex-row",
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "content": [
                                        {
                                            "component": "VImg",
                                            "props": {
                                                "src": poster,
                                                "height": 120,
                                                "width": 80,
                                                "aspect-ratio": "2/3",
                                                "class": "object-cover shadow ring-gray-500",
                                                "cover": True,
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "div",
                                    "content": [
                                        {
                                            "component": "VCardTitle",
                                            "props": {
                                                "class": "ps-1 pe-5 break-words whitespace-break-spaces"
                                            },
                                            "content": [
                                                {
                                                    "component": "a",
                                                    "props": {
                                                        "href": f"https://movie.douban.com/subject/{doubanid}",
                                                        "target": "_blank",
                                                    },
                                                    "text": title,
                                                }
                                            ],
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "pa-0 px-2"},
                                            "text": f"类型：{mtype}",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "pa-0 px-2"},
                                            "text": f"时间：{time_str}",
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                }
            )

        return [
            {
                "component": "div",
                "props": {
                    "class": "grid gap-3 grid-info-card",
                },
                "content": contents,
            }
        ]

    def stop_service(self):
        """
        停止服务
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))

    def delete_history(self, key: str, apikey: str):
        """
        删除同步历史记录
        """
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        # 历史记录
        historys = self.get_data("history")
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")
        # 删除指定记录
        historys = [h for h in historys if h.get("unique") != key]
        self.save_data("history", historys)
        return schemas.Response(success=True, message="删除成功")

    def __update_config(self):
        """
        列新配置
        """
        self.update_config(
            {
                "enabled": self._enabled,
                "cron": self._cron,
                "onlyonce": self._onlyonce,
                "vote": self._vote,
                "min_year": self._min_year,
                "title_blacklist": ",".join(self._title_blacklist),
                "rsshub": self._rsshub,
                "ranks": self._ranks,
                "rss_addrs": "\n".join(map(str, self._rss_addrs)),
                "clear": self._clear,
            }
        )

    def __refresh_rss(self):
        """
        刷新RSS
        """
        logger.info(f"开始刷新豆瓣榜单 ...")
        # 构建完整的RSS地址
        rsshub_base = self._rsshub.rstrip("/")
        rank_addrs = [
            f"{rsshub_base}{self._douban_address.get(rank)}"
            for rank in self._ranks
            if self._douban_address.get(rank)
        ]
        addr_list = self._rss_addrs + rank_addrs
        if not addr_list:
            logger.info(f"未设置榜单RSS地址")
            return
        else:
            logger.info(f"共 {len(addr_list)} 个榜单RSS地址需要刷新")

        # 读取历史记录
        if self._clearflag:
            history = []
        else:
            history: List[dict] = self.get_data("history") or []

        for addr in addr_list:
            if not addr:
                continue
            try:
                logger.info(f"获取RSS：{addr} ...")
                rss_infos = self.__get_rss_info(addr)
                if not rss_infos:
                    logger.error(f"RSS地址：{addr} ，未查询到数据")
                    continue
                else:
                    logger.info(f"RSS地址：{addr} ，共 {len(rss_infos)} 条数据")
                for rss_info in rss_infos:
                    if self._event.is_set():
                        logger.info(f"订阅服务停止")
                        return
                    mtype = None
                    title = rss_info.get("title")
                    douban_id = rss_info.get("doubanid")
                    year = rss_info.get("year")
                    # 尝试转换年份为整数
                    year_int = None
                    if year:
                        try:
                            year_int = int(year)
                        except (ValueError, TypeError):
                            pass
                    type_str = rss_info.get("type")
                    if type_str == "movie":
                        mtype = MediaType.MOVIE
                    elif type_str:
                        mtype = MediaType.TV
                    unique_flag = f"doubanrank: {title} (DB:{douban_id})"
                    # 检查是否已处理过
                    if unique_flag in [h.get("unique") for h in history]:
                        continue
                    # 检查标题黑名单
                    blacklisted = False
                    if self._title_blacklist:
                        title_lower = title.lower()
                        for keyword in self._title_blacklist:
                            if keyword.lower() in title_lower:
                                logger.info(
                                    f"{title} 标题包含黑名单关键字 '{keyword}'，跳过"
                                )
                                blacklisted = True
                                break
                    # 先检查RSS中的年份（如果有的话）
                    year_invalid = False
                    if self._min_year and year_int and year_int < self._min_year:
                        logger.info(f"{title} ({year}) 年份不符合要求")
                        year_invalid = True
                    # 如果命中黑名单或年份不符合要求，尝试识别媒体信息并取消订阅
                    if blacklisted or year_invalid:
                        # 元数据
                        meta = MetaInfo(title)
                        meta.year = year
                        if mtype:
                            meta.type = mtype
                        if meta.type not in (MediaType.MOVIE, MediaType.TV):
                            meta.type = None
                        # 匹配媒体信息
                        mediainfo = self.chain.recognize_media(meta=meta)
                        if mediainfo:
                            # 判断用户是否已经添加订阅，如果是则取消订阅
                            subscribechain = SubscribeChain()
                            if subscribechain.exists(mediainfo=mediainfo, meta=meta):
                                logger.info(
                                    f"{mediainfo.title_year} 命中黑名单/年份不符合要求，取消订阅"
                                )
                                subscribechain.delete(mediainfo=mediainfo, meta=meta)
                        continue
                    # 元数据
                    meta = MetaInfo(title)
                    meta.year = year
                    if mtype:
                        meta.type = mtype
                    if meta.type not in (MediaType.MOVIE, MediaType.TV):
                        meta.type = None
                    # 识别媒体信息
                    # if douban_id:
                    #     # 识别豆瓣信息
                    #     if settings.RECOGNIZE_SOURCE == "themoviedb":
                    #         tmdbinfo = MediaChain().get_tmdbinfo_by_doubanid(
                    #             doubanid=douban_id, mtype=meta.type
                    #         )
                    #         if not tmdbinfo:
                    #             logger.warn(
                    #                 f"未能通过豆瓣ID {douban_id} 获取到TMDB信息，标题：{title}，豆瓣ID：{douban_id}"
                    #             )
                    #             continue
                    #         meta.type = tmdbinfo.get("media_type")
                    #         mediainfo = self.chain.recognize_media(
                    #             meta=meta, tmdbid=tmdbinfo.get("id")
                    #         )
                    #         if not mediainfo:
                    #             logger.warn(
                    #                 f'TMDBID {tmdbinfo.get("id")} 未识别到媒体信息'
                    #             )
                    #             continue
                    #     else:
                    #         mediainfo = self.chain.recognize_media(
                    #             meta=meta, doubanid=douban_id
                    #         )
                    #         if not mediainfo:
                    #             logger.warn(f"豆瓣ID {douban_id} 未识别到媒体信息")
                    #             continue
                    # else:
                    # 匹配媒体信息
                    mediainfo = self.chain.recognize_media(meta=meta)
                    if not mediainfo:
                        logger.warn(
                            f"未识别到媒体信息，标题：{title}，豆瓣ID：{douban_id}"
                        )
                        continue
                    # 判断评分是否符合要求
                    if self._vote and mediainfo.vote_average < self._vote:
                        logger.info(f"{mediainfo.title_year} 评分不符合要求")
                        continue
                    # 再次确认媒体信息中的年份
                    if (
                        self._min_year
                        and mediainfo.year
                        and mediainfo.year < self._min_year
                    ):
                        logger.info(f"{mediainfo.title_year} 年份不符合要求")
                        continue
                    # 查询缺失的媒体信息
                    exist_flag, _ = DownloadChain().get_no_exists_info(
                        meta=meta, mediainfo=mediainfo
                    )
                    if exist_flag:
                        logger.info(f"{mediainfo.title_year} 媒体库中已存在")
                        continue
                    # 判断用户是否已经添加订阅
                    subscribechain = SubscribeChain()
                    if subscribechain.exists(mediainfo=mediainfo, meta=meta):
                        logger.info(f"{mediainfo.title_year} 订阅已存在")
                        continue
                    # 添加订阅
                    subscribechain.add(
                        title=mediainfo.title,
                        year=mediainfo.year,
                        mtype=mediainfo.type,
                        tmdbid=mediainfo.tmdb_id,
                        season=meta.begin_season,
                        exist_ok=True,
                        username="豆瓣榜单v2",
                    )
                    # 存储历史记录
                    history.append(
                        {
                            "title": title,
                            "type": mediainfo.type.value,
                            "year": mediainfo.year,
                            "poster": mediainfo.get_poster_image(),
                            "overview": mediainfo.overview,
                            "tmdbid": mediainfo.tmdb_id,
                            "doubanid": douban_id,
                            "time": datetime.datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            "unique": unique_flag,
                        }
                    )
            except Exception as e:
                logger.error(str(e))

        # 保存历史记录
        self.save_data("history", history)
        # 缓存只清理一次
        self._clearflag = False
        logger.info(f"所有榜单RSS刷新完成")

    def __get_rss_info(self, addr) -> List[dict]:
        """
        获取RSS
        """
        try:
            if self._proxy:
                ret = RequestUtils(proxies=settings.PROXY).get_res(addr)
            else:
                ret = RequestUtils().get_res(addr)
            if not ret:
                return []
            ret_xml = ret.text
            ret_array = []
            # 解析XML
            dom_tree = xml.dom.minidom.parseString(ret_xml)
            rootNode = dom_tree.documentElement
            items = rootNode.getElementsByTagName("item")
            for item in items:
                try:
                    rss_info = {}

                    # 标题
                    title = DomUtils.tag_value(item, "title", default="")
                    # 链接
                    link = DomUtils.tag_value(item, "link", default="")
                    # 年份
                    description = DomUtils.tag_value(item, "description", default="")

                    if not title and not link:
                        logger.warn(f"条目标题和链接均为空，无法处理")
                        continue
                    rss_info["title"] = title
                    rss_info["link"] = link

                    doubanid = re.findall(r"/(\d+)(?=/|$)", link)
                    if doubanid:
                        doubanid = doubanid[0]
                    if doubanid and not str(doubanid).isdigit():
                        logger.warn(f"解析的豆瓣ID格式不正确：{doubanid}")
                        continue
                    rss_info["doubanid"] = doubanid

                    # 匹配4位独立数字1900-2099年
                    year = re.findall(r"\b(19\d{2}|20\d{2})\b", description)
                    if year:
                        rss_info["year"] = year[0]

                    # 返回对象
                    ret_array.append(rss_info)
                except Exception as e1:
                    logger.error("解析RSS条目失败：" + str(e1))
                    continue
            return ret_array
        except Exception as e:
            logger.error("获取RSS失败：" + str(e))
            return []
