import datetime
import re
from collections import defaultdict
from typing import Any, List, Optional, Tuple, Dict
from app.chain import ChainBase
from app.chain.download import DownloadChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfo
from app.log import logger
from app.plugins.customplugin.task import UserTaskBase
from app.schemas import MediaType
from app.schemas.types import NotificationType
import feedparser
import pytz
import requests
from threading import Event


class DoubanRankV2(UserTaskBase):
    _rsshub = "https://rsshub.app"
    _douban_address = {
        "movie_real_time_hot": "/douban/movie/playing/hot",
        "movie_showing_sooner": "/douban/movie/playing/soon",
        "movie_weekly_best": "/douban/movie/weekly",
        "movie_top250": "/douban/movie/top250",
        "movie_chinese_top": "/douban/movie/top/chinese",
        "movie_us_top": "/douban/movie/top/usa",
        "movie_love_top": "/douban/movie/top/love",
        "movie_scifi_top": "/douban/movie/top/scifi",
        "movie_cartoon_top": "/douban/movie/top/cartoon",
        "movie_action_top": "/douban/movie/top/action",
        "movie_comedy_top": "/douban/movie/top/comedy",
        "movie_horror_top": "/douban/movie/top/horror",
        "movie_suspense_top": "/douban/movie/top/suspense",
        "movie_mystery_top": "/douban/movie/top/mystery",
        "movie_fantasy_top": "/douban/movie/top/fantasy",
        "movie_animation_top": "/douban/movie/top/animation",
        "movie_documentary_top": "/douban/movie/top/documentary",
        "movie_short_top": "/douban/movie/top/short",
        "movie_erotic_top": "/douban/movie/top/erotic",
        "movie_home_top": "/douban/movie/top/home",
        "movie_black_top": "/douban/movie/top/black",
        "tv_real_time_hot": "/douban/tv/playing/hot",
        "tv_showing_sooner": "/douban/tv/playing/soon",
        "tv_chinese_top": "/douban/tv/top/chinese",
        "tv_american_top": "/douban/tv/top/american",
        "tv_japan_top": "/douban/tv/top/japan",
        "tv_korean_top": "/douban/tv/top/korean",
        "tv_animation_top": "/douban/tv/top/animation",
    }
    _ranks = ["movie_real_time_hot", "tv_real_time_hot"]
    _rss_addrs = []
    _onlyonce = False
    _cron = "0 0 * * *"
    _vote = 0.0
    _clearflag = False
    _scheduler = None
    _event = Event()

    downloadchain: DownloadChain = None
    subscribechain: SubscribeChain = None
    chain: ChainBase = None

    def start(self):
        """
        开始任务时调用此方法
        """
        logger.info("DoubanRankV2 开始运行")
        self.downloadchain = DownloadChain()
        self.subscribechain = SubscribeChain()
        self.chain = ChainBase()
        self.__refresh_rss()

    def stop(self):
        """
        停止任务时调用此方法
        """
        logger.info("DoubanRankV2 停止运行")
        self._event.set()

    def __refresh_rss(self):
        """
        刷新RSS
        """
        logger.info(f"开始刷新豆瓣榜单 ...")
        rsshub_base = self._rsshub.rstrip('/')
        rank_addrs = [f"{rsshub_base}{self._douban_address.get(rank)}" for rank in self._ranks if self._douban_address.get(rank)]
        addr_list = self._rss_addrs + rank_addrs
        if not addr_list:
            logger.info(f"未设置榜单RSS地址")
            return
        else:
            logger.info(f"共 {len(addr_list)} 个榜单RSS地址需要刷新")

        history = []
        unique_history = set()

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
                    title = rss_info.get('title')
                    douban_id = rss_info.get('doubanid')
                    year = rss_info.get('year')
                    type_str = rss_info.get('type')
                    unique_flag = f"doubanrankv2: {title} (DB:{douban_id})"
                    logger.info(f"\n")
                    logger.info(f"标题：{title}")

                    if unique_flag in unique_history:
                        logger.info(f"{title} 已处理过")
                        continue

                    meta = MetaInfo(title)
                    if year:
                        meta.year = year
                    if type_str == "movie":
                        meta.type = MediaType.MOVIE
                    elif type_str:
                        meta.type = MediaType.TV

                    if douban_id:
                        mediainfo = self.chain.recognize_media(meta=meta, doubanid=douban_id)
                    else:
                        mediainfo: MediaInfo = self.chain.recognize_media(meta=meta)

                    if not mediainfo:
                        logger.warn(f'未识别到媒体信息，标题：{title}，豆瓣ID：{douban_id}')
                        continue

                    if self._vote and mediainfo.vote_average < self._vote:
                        logger.info(f'{mediainfo.title_year} 评分不符合要求')
                        continue

                    exist_flag, _ = self.downloadchain.get_no_exists_info(meta=meta, mediainfo=mediainfo)
                    if exist_flag:
                        logger.info(f'{mediainfo.title_year} 媒体库中已存在')
                        continue

                    if self.subscribechain.exists(mediainfo=mediainfo, meta=meta):
                        logger.info(f'{mediainfo.title_year} 订阅已存在')
                        continue

                    sid, msg = self.subscribechain.add(
                        title=mediainfo.title,
                        year=mediainfo.year or year or "",
                        mtype=mediainfo.type,
                        tmdbid=mediainfo.tmdb_id,
                        season=meta.begin_season,
                        exist_ok=True,
                        username="豆瓣榜单"
                    )
                    if not sid:
                        logger.error(f"{title} 订阅失败：{msg}")
                        continue

                    self.__post_message(
                        mtype=NotificationType.Subscribe,
                        title=f"{mediainfo.title_year} 已添加订阅",
                        text=f"{rss_info.get('description', '') or mediainfo.overview or ''}\n{self.__build_douban_dispatch_link(rss_info.get('link', ''))}\n\n[豆瓣榜单V2]",
                        image=mediainfo.get_message_image(),
                        link=settings.MP_DOMAIN("#/subscribe")
                    )
                    logger.info(f"{title} 已添加订阅")

                    history.append({
                        "title": title,
                        "type": mediainfo.type.value,
                        "year": mediainfo.year,
                        "poster": mediainfo.get_poster_image(),
                        "overview": mediainfo.overview,
                        "tmdbid": mediainfo.tmdb_id,
                        "doubanid": douban_id,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "unique": unique_flag
                    })
                    unique_history.add(unique_flag)
            except Exception as e:
                logger.error(str(e))

        logger.info(f"所有榜单RSS刷新完成")

    @staticmethod
    def __get_rss_info(url: str) -> List[dict]:
        """
        获取RSS信息
        """
        try:
            ret_array = []
            rss = feedparser.parse(url)
            for entry in rss.entries:
                pubdate = datetime.datetime(*entry.published_parsed[:6])
                ret = {'title': entry.title,
                       'description': entry.get("description", ""),
                       'link': entry.get("link", ""),
                       'pubdate': pubdate,
                       }
                if ret.get('description'):
                    match = re.search(r'<img\s+[^>]*src="([^"]+)"[^>]*>', ret['description'])
                    if match:
                        ret['image'] = match.group(1)
                    pattern = r"""
                        评分.*?
                        <strong.*?>(?P<vote>[\d.]+)</strong>.*?
                        /\s*10.*?
                        /\s*(?P<type>\w+)\s*/.*?
                        /\s*(?P<year>\d{4})\s*/
                    """
                    match_type = re.search(pattern, ret['description'], re.VERBOSE)
                    if match_type:
                        ret['type'] = match_type.group('type')
                        ret['year'] = match_type.group('year')
                        if ret['year'] and len(ret['year']) > 4:
                            ret['year'] = ret['year'][:4]
                        ret['vote'] = match_type.group('vote')
                    match_id = re.search(r"subject/(\d+)", ret['link'])
                    if match_id:
                        ret['doubanid'] = match_id.group(1)
                ret_array.append(ret)
            return ret_array
        except Exception as e:
            logger.error("获取RSS失败：" + str(e))
            return []

    @staticmethod
    def __build_douban_dispatch_link(link: str) -> str:
        if not link:
            return ""
        match = re.search(r"/(\d+)(?=/|$)", link)
        if not match:
            return link
        subject_id = match.group(1)
        return f"https://www.douban.com/doubanapp/dispatch?uri=/movie/{subject_id}?from=mdouban&open=app"

    def __post_message(self, mtype: NotificationType, title: str, text: str = "",
                       image: str = None, link: str = None, userid: str = None):
        """
        发送消息通知
        """
        try:
            from app.chain.message import MessageChain
            MessageChain().post_message(
                mtype=mtype,
                title=title,
                text=text,
                image=image,
                link=link,
                userid=userid
            )
        except Exception as e:
            logger.error(f"发送消息失败：{str(e)}")
