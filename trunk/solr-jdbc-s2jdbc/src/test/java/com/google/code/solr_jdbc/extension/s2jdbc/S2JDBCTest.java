package com.google.code.solr_jdbc.extension.s2jdbc;

import java.util.List;

import javax.transaction.UserTransaction;

import org.seasar.extension.jdbc.JdbcManager;
import org.seasar.extension.unit.S2TestCase;
import org.seasar.framework.util.tiger.CollectionsUtil;

import com.google.code.solr_jdbc.extension.s2jdbc.entity.Player;
import com.google.code.solr_jdbc.extension.s2jdbc.service.PlayerService;


public class S2JDBCTest extends S2TestCase {
	PlayerService playerService;
	JdbcManager jdbcManager;
	UserTransaction userTransaction;

	@Override
	public void setUp() {
		include("app.dicon");
	}


	public void testSearchSimple() throws Exception {
		try {
			jdbcManager.updateBySql("DROP TABLE PLAYER").execute();
		} catch(Exception ignore) {
		}
		jdbcManager.updateBySql("CREATE TABLE PLAYER(PLAYER_ID number, TEAM varchar(10), PLAYER_NAME varchar(50), POSITION varchar(10) ARRAY, REGISTERED_AT DATE)").execute();
		Player player1 = new Player();
		player1.playerId = 1;
		player1.playerName = "高橋慶彦";
		player1.team = "カープ";
		player1.position = CollectionsUtil.newArrayList();
		player1.position.add("二塁手");
		player1.position.add("遊撃手");
		userTransaction.begin();
		playerService.insert(player1);

		Player player2 = new Player();
		player2.playerId = 2;
		player2.playerName = "山崎隆造";
		player2.team = "カープ";
		player2.position = null;
		playerService.insert(player2);
		userTransaction.commit();

		List<Player> playerList = playerService.findAll();
		assertEquals(2, playerList.size());
		Player player = playerList.get(0);
		assertEquals("Entityに値が入っている", "高橋慶彦", player.playerName);
		assertNull("設定しなかったところはnullが入る", player.registeredAt);

		player.playerName = "野村謙二郎";
		userTransaction.begin();
		int res = playerService.update(player);
		userTransaction.commit();

		assertEquals("1件更新される", 1, res);

		player = playerService.find(1);
		assertEquals("playerNameの値が更新されている", "野村謙二郎", player.playerName);

	}

	public void test2WaySQL() throws Exception {
		try {
			jdbcManager.updateBySql("DROP TABLE PLAYER").execute();
		} catch(Exception ignore) {

		}
		jdbcManager.updateBySql("CREATE TABLE PLAYER(PLAYER_ID number, TEAM varchar(10), PLAYER_NAME varchar(50), POSITION varchar(10) ARRAY, REGISTERED_AT DATE)").execute();
		Player player1 = new Player();
		player1.playerId = 1;
		player1.playerName = "高橋慶彦";
		player1.team = "カープ";
		player1.position = CollectionsUtil.newArrayList();
		player1.position.add("二塁手");
		player1.position.add("遊撃手");
		userTransaction.begin();
		playerService.insert(player1);

		Player player2 = new Player();
		player2.playerId = 2;
		player2.playerName = "山崎隆造";
		player2.team = "カープ";
		player2.position = null;
		playerService.insert(player2);
		userTransaction.commit();

		List<Player> playerList = playerService.findBySql();
		assertEquals("二塁手", playerList.get(0).position.get(0));
		assertEquals("遊撃手", playerList.get(0).position.get(1));

	}
}
