package solr.jdbc.extension.s2jdbc;

import java.util.List;

import javax.transaction.UserTransaction;

import org.apache.commons.collections.CollectionUtils;
import org.seasar.extension.jdbc.EntityMetaFactory;
import org.seasar.extension.jdbc.JdbcManager;
import org.seasar.extension.unit.S2TestCase;
import org.seasar.framework.beans.util.BeanMap;
import org.seasar.framework.util.tiger.CollectionsUtil;

import solr.jdbc.extension.s2jdbc.entity.Player;
import solr.jdbc.extension.s2jdbc.meta.SolrPropertyMetaFactoryImpl;
import solr.jdbc.extension.s2jdbc.service.PlayerService;

public class S2JDBCTest extends S2TestCase {
	PlayerService playerService;
	JdbcManager jdbcManager;
	UserTransaction userTransaction;

	@Override
	public void setUp() {
		include("app.dicon");
	}

	
	public void testSearchSimple() throws Exception {
		EntityMetaFactory entityMetaFactory = (EntityMetaFactory)getComponent("entityMetaFactory");
		entityMetaFactory.getEntityMeta(Player.class);
		jdbcManager.updateBySql("DROP TABLE PLAYER").execute();
		jdbcManager.updateBySql("CREATE TABLE PLAYER(PLAYER_ID number, TEAM varchar(10), PLAYER_NAME varchar(50), POSITION varchar(10) ARRAY, REGISTERED_AT DATE)").execute();
		Player player1 = new Player();
		player1.playerId = 1;
		player1.playerName = "高橋慶彦";
		player1.team = "カープ";
		player1.position = CollectionsUtil.newArrayList();
		player1.position.add("遊撃手");
		player1.position.add("二塁手");
		userTransaction.begin();
		playerService.insert(player1);
		userTransaction.commit();

		List<Player> playerList = playerService.findAll();
		assertEquals(1, playerList.size());
		Player player = playerList.get(0);
		assertEquals("Entityに値が入っている", "高橋慶彦", player.playerName);
		assertNull("設定しなかったところはnullが入る", player.registeredAt);
		assertEquals("マルチバリューの項目にListが入る", 2, player.position.size());
		
		player.playerName = "野村謙二郎";
		int res = playerService.update(player);
		
		assertEquals("1件更新される", 1, res);
		
		player = playerService.find(1);
		assertEquals("playerNameの値が更新されている", "野村謙二郎", player.playerName);
		
	}
}
