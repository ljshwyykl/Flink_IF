package com.knn3.rt.scene.ifcondition.service;

import com.knn3.rt.scene.ifcondition.constant.Cons;
import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.ImpossibleFinance;
import com.knn3.rt.scene.ifcondition.model.Status;

import java.util.UUID;

/**
 * @Author apophis
 * @File TransService
 * @Time 2022/3/26 22:52
 * @Description 工程描述
 */
public class TransService {
    public static Status ofStatusMsg(Balance balance, UUID id) {
        Status status = new Status();
        status.setId(UUID.randomUUID());
        status.setDcpTable(Cons.DCP_TABLE);
        status.setUid(id);
        status.setAddress(balance.getAddress());
        return status;
    }

    public static ImpossibleFinance ofFinanceMsg(Balance balance) {
        String contractId = "0xB0e1fc65C1a741b4662B813eB787d369b8614Af1".toLowerCase();
        ImpossibleFinance finance = new ImpossibleFinance();
        finance.setId(UUID.randomUUID());
        finance.setAddress(balance.getAddress());
        finance.setChainId("56");
        finance.setContractId(contractId);
        finance.setTokenSymbol("IF");
        finance.setTokenName("Impossible Finance");
        finance.setCampaignId(contractId);
        finance.setCampaignName(Cons.DCP_TABLE);
        finance.setBlockNumber(balance.getBlockNumber());
        finance.setBalance(balance.getBalance());
        finance.setIfFansTokenThreshold(true);
        return finance;
    }
}
