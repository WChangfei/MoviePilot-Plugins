import datetime
from threading import Event
from typing import Tuple, List, Dict, Any, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfo
from app.db.mediaserver_oper import MediaServerOper
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaType, ServiceInfo


class PlexComplete(_PluginBase):
    plugin_name = "Plex剧集补全"
    plugin_desc = "检查媒体库中的电视剧，对比TMDB集数，自动添加订阅补全缺失剧集"
    plugin_icon = "movie.jpg"
    plugin_version = "1.1.7"
    plugin_author = "WChangfei"
    author_url = ""
    plugin_config_prefix = "plexcomplete_"
    plugin_order = 7
    auth_level = 1

    _event = Event()
    _scheduler = None
    _enabled = False
    _cron = ""
    _onlyonce = False
    _libraries: List[str] = []
    _title_blacklist: List[str] = []
    mediaserver_helper: MediaServerHelper = None

    mediachain: MediaChain = None
    subscribechain: SubscribeChain = None
    downloadchain: DownloadChain = None
    mediaserveroper: MediaServerOper = None

    def init_plugin(self, config: dict = None):
        self.mediachain = MediaChain()
        self.subscribechain = SubscribeChain()
        self.downloadchain = DownloadChain()
        self.mediaserveroper = MediaServerOper()
        self.mediaserver_helper = MediaServerHelper()

        if config:
            self._enabled = config.get("enabled", False)
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce", False)
            self._libraries = config.get("libraries") or []
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

        self.stop_service()

        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._onlyonce:
                logger.info("Plex剧集补全服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self.__check_library,
                    trigger="date",
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                    + datetime.timedelta(seconds=3),
                )

            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(
                        func=self.__check_library,
                        trigger=CronTrigger.from_crontab(self._cron),
                        name="Plex剧集补全",
                    )
                except Exception as err:
                    logger.error(f"Plex剧集补全服务启动失败：{err}")
                    self.systemmessage.put(f"Plex剧集补全服务启动失败：{err}")

            if self._onlyonce:
                self._onlyonce = False
                self.__update_config()

            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [
                {
                    "id": "PlexComplete",
                    "name": "Plex剧集补全服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.__check_library,
                    "kwargs": {},
                }
            ]
        elif self._enabled:
            return [
                {
                    "id": "PlexComplete",
                    "name": "Plex剧集补全服务",
                    "trigger": CronTrigger.from_crontab("0 9 * * *"),
                    "func": self.__check_library,
                    "kwargs": {},
                }
            ]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        library_options = self.__get_library_options()
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
                                            "placeholder": "5位cron表达式",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "libraries",
                                            "label": "媒体库选择",
                                            "items": library_options,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "placeholder": "选择要检查的媒体库，不选择则检查所有",
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
                                "props": {"cols": 12},
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
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "cron": "",
            "onlyonce": False,
            "libraries": [],
            "title_blacklist": "",
        }

    def service_infos(
        self, name_filters: Optional[List[str]] = None
    ) -> Optional[Dict[str, ServiceInfo]]:
        """
        服务信息
        """
        services = self.mediaserver_helper.get_services(
            name_filters=name_filters, type_filter="plex"
        )
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if (
                hasattr(service_info.instance, "is_inactive")
                and service_info.instance.is_inactive()
            ):
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器，请检查配置")
            return None

        return active_services

    def __get_library_options(self) -> List[Dict[str, Any]]:
        """获取媒体库选项列表"""
        library_options = []
        service_infos = self.service_infos()
        if not service_infos:
            return library_options

        # 获取所有媒体库
        for service in service_infos.values():
            plex = service.instance
            if not plex or not hasattr(plex, "get_plex") or not plex.get_plex():
                continue
            plex_server = plex.get_plex()
            try:
                libraries = sorted(plex_server.library.sections(), key=lambda x: x.key)
                # 遍历媒体库，创建字典并添加到列表中
                for library in libraries:
                    # 仅支持剧集媒体库
                    if library.TYPE != "show":
                        continue
                    library_dict = {
                        "title": f"{service.name} - {library.key}. {library.title} ({library.TYPE})",
                        "value": f"{service.name}.{library.key}",
                    }
                    library_options.append(library_dict)
            except Exception as e:
                logger.error(f"获取Plex媒体库失败：{str(e)}")
        return library_options

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            logger.error(str(e))

    def __update_config(self):
        self.update_config(
            {
                "enabled": self._enabled,
                "cron": self._cron,
                "onlyonce": self._onlyonce,
                "libraries": self._libraries,
                "title_blacklist": ",".join(self._title_blacklist),
            }
        )

    def __get_library_name(self, library_id: str) -> str:
        """根据媒体库ID获取媒体库名称"""
        try:
            service_infos = self.service_infos()
            if not service_infos:
                return library_id

            for service in service_infos.values():
                plex = service.instance
                if not plex or not hasattr(plex, "get_plex") or not plex.get_plex():
                    continue
                plex_server = plex.get_plex()
                try:
                    libraries = plex_server.library.sections()
                    for library in libraries:
                        if str(library.key) == library_id:
                            return f"{service.name} - {library.title}"
                except Exception:
                    pass
        except Exception:
            pass
        return library_id

    def __check_library(self):
        logger.info("开始检查媒体库剧集...")

        try:
            from app.db import ScopedSession
            from app.db.models.mediaserver import MediaServerItem

            db = ScopedSession()
            try:
                query = db.query(MediaServerItem).filter(
                    MediaServerItem.item_type == "电视剧"
                )

                # 应用媒体库筛选
                if self._libraries:
                    # 提取媒体库ID列表
                    library_ids = []
                    for lib in self._libraries:
                        try:
                            # 格式：服务名.媒体库ID
                            if "." in lib:
                                library_id = lib.split(".")[-1]
                                library_ids.append(library_id)
                            else:
                                library_ids.append(lib)
                        except Exception:
                            pass
                    if library_ids:
                        query = query.filter(MediaServerItem.library.in_(library_ids))
                        logger.info(f"筛选媒体库ID：{library_ids}")

                items = query.all()

                if not items:
                    logger.info("未找到媒体库中的电视剧")
                    return

                # 按媒体库分组
                library_items = {}
                for item in items:
                    library = item.library or "未知"
                    if library not in library_items:
                        library_items[library] = []
                    library_items[library].append(item)

                logger.info(
                    f"共发现 {len(library_items)} 个媒体库，分别是：{list(library_items.keys())}"
                )

                # 处理每个媒体库
                for library_id, lib_items in library_items.items():
                    try:
                        library_name = self.__get_library_name(library_id)
                        logger.info(
                            f"开始处理媒体库 {library_name}，共 {len(lib_items)} 部电视剧"
                        )
                        processed_count = 0

                        for item in lib_items:
                            if self._event.is_set():
                                logger.error(
                                    f"服务停止，媒体库 {library_name} 未完成处理，已处理 {processed_count} 部，剩余 {len(lib_items) - processed_count} 部"
                                )
                                break

                            try:
                                self.__process_item(item)
                                processed_count += 1
                            except Exception as e:
                                logger.error(
                                    f"处理电视剧 {item.title} 时出错：{str(e)}"
                                )
                                logger.warning(
                                    f"媒体库 {library_name} 继续处理，已处理 {processed_count} 部，当前失败剧集：{item.title}"
                                )

                        logger.info(
                            f"媒体库 {library_name} 处理完成，共处理 {processed_count} 部电视剧"
                        )
                    except Exception as e:
                        logger.error(f"处理媒体库 {library_id} 时出错：{str(e)}")
                        logger.warning(f"跳过媒体库 {library_id}，继续处理其他媒体库")

                logger.info("所有媒体库剧集检查完成")
            finally:
                db.close()

        except Exception as e:
            logger.error(f"检查媒体库时出错：{str(e)}")

    def __process_item(self, item):
        title = item.title
        year = item.year
        tmdbid = item.tmdbid
        item_id = item.item_id

        # 检查标题黑名单
        if self._title_blacklist:
            title_lower = title.lower()
            for keyword in self._title_blacklist:
                if keyword.lower() in title_lower:
                    logger.info(f"{title} 标题包含黑名单关键字 '{keyword}'，跳过")
                    return

        logger.info(f"处理电视剧：{title} ({year})")

        meta = MetaInfo(title)
        if year:
            meta.year = year
        meta.type = MediaType.TV

        mediainfo = None
        if tmdbid:
            mediainfo = self.mediachain.recognize_media(meta=meta, tmdbid=tmdbid)
        else:
            mediainfo = self.mediachain.recognize_media(meta=meta)

        if not mediainfo:
            logger.warning(f"未识别到电视剧媒体信息：{title}")
            return

        if not mediainfo.seasons:
            logger.warning(f"未获取到电视剧季集信息：{title}")
            return

        plex_seasoninfo = item.seasoninfo or {}

        # 只处理媒体库中已存在的季
        for season_str in plex_seasoninfo.keys():
            try:
                season_num = int(season_str)
            except (ValueError, TypeError):
                continue

            # 检查TMDB中是否有这个季的信息
            tmdb_episodes = mediainfo.seasons.get(season_num)
            if not tmdb_episodes:
                logger.info(f"{title} 第{season_num}季在TMDB中未找到信息，跳过")
                continue

            total_episodes = len(tmdb_episodes)
            if total_episodes == 0:
                continue

            logger.info(f"检查 {title} 第{season_num}季，TMDB共{total_episodes}集")

            plex_episodes = plex_seasoninfo.get(season_str) or []
            plex_episode_count = len(plex_episodes)
            logger.info(f"{title} 第{season_num}季，媒体库现有{plex_episode_count}集")

            if plex_episode_count >= total_episodes:
                logger.info(f"{title} 第{season_num}季已完整，跳过")
                continue

            logger.info(f"{title} 第{season_num}季有缺失集数")

            meta_season = MetaInfo(title)
            if year:
                meta_season.year = year
            meta_season.type = MediaType.TV
            meta_season.begin_season = season_num

            exist_flag, no_exists = self.downloadchain.get_no_exists_info(
                meta=meta_season, mediainfo=mediainfo
            )

            if exist_flag:
                logger.info(f"{title} 第{season_num}季媒体库实际已完整，跳过")
                continue

            if self.subscribechain.exists(mediainfo=mediainfo, meta=meta_season):
                logger.info(f"{title} 第{season_num}季已存在订阅")
                continue

            logger.info(f"为 {title} 第{season_num}季添加订阅")
            sid, err_msg = self.subscribechain.add(
                title=mediainfo.title,
                year=mediainfo.year,
                mtype=MediaType.TV,
                tmdbid=mediainfo.tmdb_id,
                doubanid=mediainfo.douban_id,
                season=season_num,
                exist_ok=True,
                username="PlexComplete",
            )

            if sid:
                logger.info(f"{title} 第{season_num}季订阅添加成功")
            else:
                logger.error(f"{title} 第{season_num}季订阅添加失败：{err_msg}")
