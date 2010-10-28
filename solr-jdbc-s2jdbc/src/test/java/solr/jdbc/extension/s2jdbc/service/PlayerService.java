package solr.jdbc.extension.s2jdbc.service;

import java.util.List;

import org.seasar.extension.jdbc.service.S2AbstractService;
import org.seasar.framework.beans.util.BeanMap;

import solr.jdbc.extension.s2jdbc.entity.Player;

public class PlayerService extends S2AbstractService<Player> {

	public Player find(int id) {
		BeanMap conditions = new BeanMap();
		conditions.put("playerId", 1);
		List<Player> playerList = findByCondition(conditions);
		if(playerList.size() > 0) {
			return playerList.get(0);
		}
		return null;
	}

}
