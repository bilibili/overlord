package redis

// // IsBatch impl the proto.protoRequest and check if the command is batchable
// func (rr *Command) IsBatch() bool {
// 	return rr.batchStep != defaultBatchStep
// }

// // // Batch impl the proto.protoRequest and split the command into divided part.
// // func (rr *Command) Batch() ([]proto.Request, *proto.Response) {
// // 	if rr.batchStep == defaultBatchStep {
// // 		// batch but only split one
// // 		return rr.batchOne()
// // 	}

// // 	return rr.batchByStep(rr.batchStep)
// // }

// // func (rr *Command) batchOne() ([]proto.Request, *proto.Response) {
// // 	reqs := []proto.Request{
// // 		proto.Request{
// // 			Type: proto.CacheTypeRedis,
// // 		},
// // 	}
// // 	reqs[0].WithProto(rr)
// // 	response := &proto.Response{
// // 		Type: proto.CacheTypeRedis,
// // 	}
// // 	return reqs, response
// // }

// // func (rr *Command) batchByStep(step int) ([]proto.Request, *proto.Response) {
// // 	// NEEDTEST(wayslog): we assume that the request is full.

// // 	// trim cmd
// // 	cmd := rr.Cmd()
// // 	mergeType := getMergeType(cmd)

// // 	slice := rr.respObj.slice()[1:]

// // 	items := (rr.respObj.Len() - 1) / step
// // 	resps := make([]proto.Request, items)

// // 	batchCmd := getBatchCmd(cmd)

// // 	bcmdResp := newRespString(batchCmd)
// // 	bcmdType := getCmdType(batchCmd)

// // 	for i := 0; i < items; i++ {
// // 		// keyPos := i*step
// // 		// argsBegin := i*step+1
// // 		// argsEnd := i*step + step -1
// // 		r := newRespArrayWithCapcity(step + 1)
// // 		r.replace(0, bcmdResp)
// // 		r.replaceAll(1, slice[i*step:(i+1)*step])

// // 		req := proto.Request{Type: proto.CacheTypeRedis}
// // 		req.WithProto(&Command{
// // 			respObj:   r,
// // 			cmdType:   bcmdType,
// // 			batchStep: defaultBatchStep,
// // 		})
// // 		resps[i] = req
// // 	}

// // 	response := &proto.Response{
// // 		Type: proto.CacheTypeRedis,
// // 	}
// // 	response.WithProto(newRResponse(mergeType, nil))
// // 	return resps, response
// // }

// // RResponse is the redis response protocol type.
// type RResponse struct {
// 	respObj *resp

// 	mergeType MergeType
// }

// func newRResponse(mtype MergeType, robj *resp) *RResponse {
// 	return &RResponse{
// 		mergeType: mtype,
// 		respObj:   robj,
// 	}
// }

// // Merge impl the proto.Merge interface
// func (rr *RResponse) Merge(subs []proto.Request) {
// 	switch rr.mergeType {
// 	case MergeTypeBasic:
// 		srr, ok := subs[0].Resp.Proto().(*RResponse)
// 		if !ok {
// 			// TOOD(wayslog): log it
// 			return
// 		}
// 		rr.respObj = srr.respObj
// 	case MergeTypeJoin:
// 		rr.mergeJoin(subs)
// 	case MergeTypeCount:
// 		rr.mergeCount(subs)
// 	case MergeTypeOk:
// 		rr.mergeOk(subs)
// 	}
// }

// func (rr *RResponse) mergeJoin(subs []proto.Request) {
// 	if rr.respObj == nil {
// 		rr.respObj = newRespArrayWithCapcity(len(subs))
// 	}
// 	if rr.respObj.isNull() {
// 		rr.respObj.array = make([]*resp, len(subs))
// 	}
// 	for idx, sub := range subs {
// 		srr, ok := sub.Resp.Proto().(*RResponse)
// 		if !ok {
// 			// TODO(wayslog): log it
// 			continue
// 		}
// 		rr.respObj.replace(idx, srr.respObj)
// 	}
// }

// // IsRedirect check if response type is Redis Error
// // and payload was prefix with "ASK" && "MOVED"
// func (rr *RResponse) IsRedirect() bool {
// 	if rr.respObj.rtype != respError {
// 		return false
// 	}
// 	if rr.respObj.data == nil {
// 		return false
// 	}

// 	return bytes.HasPrefix(rr.respObj.data, movedBytes) ||
// 		bytes.HasPrefix(rr.respObj.data, askBytes)
// }

// // RedirectTriple will check and send back by is
// // first return variable which was called as redirectType maybe return ASK or MOVED
// // second is the slot of redirect
// // third is the redirect addr
// // last is the error when parse the redirect body
// func (rr *RResponse) RedirectTriple() (redirect string, slot int, addr string, err error) {
// 	fields := strings.Fields(string(rr.respObj.data))
// 	if len(fields) != 3 {
// 		err = ErrRedirectBadFormat
// 		return
// 	}
// 	redirect = fields[0]
// 	addr = fields[2]
// 	ival, parseErr := strconv.Atoi(fields[1])

// 	slot = ival
// 	err = parseErr
// 	return
// }

// func (rr *RResponse) mergeCount(subs []proto.Request) {
// 	count := 0
// 	for _, sub := range subs {
// 		if err := sub.Resp.Err(); err != nil {
// 			// TODO(wayslog): log it
// 			continue
// 		}
// 		ssr, ok := sub.Resp.Proto().(*RResponse)
// 		if !ok {
// 			continue
// 		}
// 		ival, err := strconv.Atoi(string(ssr.respObj.data))
// 		if err != nil {
// 			continue
// 		}
// 		count += ival
// 	}
// 	rr.respObj = newRespInt(count)
// }

// func (rr *RResponse) mergeOk(subs []proto.Request) {
// 	for _, sub := range subs {
// 		if err := sub.Resp.Err(); err != nil {
// 			// TODO(wayslog): set as bad response
// 			return
// 		}
// 	}
// 	rr.respObj = newRespString("OK")
// }
