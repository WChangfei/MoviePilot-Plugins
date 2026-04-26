from typing import Any, Dict, List, Tuple

from app.plugins import _PluginBase


class DouBanRankV2(_PluginBase):
    # 插件在界面中的展示名称
    plugin_name = "我的插件"
    # 插件描述
    plugin_desc = "一个最小可运行的 V2 插件示例。"
    # 插件图标
    plugin_icon = "Moviepilot_A.png"
    # 插件版本，必须和 package.v2.json 中保持一致
    plugin_version = "1.0.0"
    # 作者信息
    plugin_author = "WChangFei"
    author_url = "https://github.com/honue"
    # 配置项前缀，建议保持唯一，避免与其他插件冲突
    plugin_config_prefix = "doubanrankv2_"  
    # 插件加载顺序，数值越小越早
    plugin_order = 50
    # 插件可见权限级别
    auth_level = 1

    # 运行时状态字段
    _enabled = False
    _message = "插件尚未初始化"

    def init_plugin(self, config: dict = None):
        """根据当前配置初始化插件。"""
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._message = config.get("message") or "Hello MoviePilot"

    def get_state(self) -> bool:
        """返回插件当前是否启用。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """没有远程命令时直接返回空列表。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """没有插件 API 时直接返回空列表。"""
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回配置页 JSON 和默认配置模型。"""
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
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "message",
                                            "label": "展示文本",
                                        },
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ], {
            "enabled": False,
            "message": "Hello MoviePilot",
        }

    def get_page(self) -> List[dict]:
        """返回详情页 JSON。"""
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": self._message,
                },
            }
        ]

    def stop_service(self):
        """没有后台任务时可以留空。"""
        pass