/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepSyncToDb.hpp
 * @brief 
 * @author   Bwar
 * @date:    2018年7月15日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_STEPSYNCTODB_HPP_
#define MYDIS_STEPSYNCTODB_HPP_

#include <string>
#include "actor/step/PbStep.hpp"
#include "SessionSyncDbData.hpp"

namespace mydis
{

class SessionSyncDbData;

class StepSyncToDb: public neb::PbStep
{
public:
    StepSyncToDb(SessionSyncDbData* pSyncSession, const MsgHead& oInMsgHead, const MsgBody& oInMsgBody);
    virtual ~StepSyncToDb();

    virtual neb::E_CMD_STATUS Emit(int iErrno, const std::string& strErrMsg = "", void* data = NULL);

    virtual neb::E_CMD_STATUS Callback(
                    std::shared_ptr<neb::SocketChannel> pChannel,
                    int32 iCmd, uint32 uiSeq,
                    const MsgBody& oInMsgBody,
                    void* data = NULL);

    virtual neb::E_CMD_STATUS Timeout();

private:
    SessionSyncDbData* m_pSyncSession;
    MsgHead m_oMsgHead;
    MsgBody m_oMsgBody;
};

} /* namespace mydis */

#endif /* MYDIS_STEPSYNCTODB_HPP_ */
