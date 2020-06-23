package chunk

import (
	"errors"
	"fmt"
	"overlord/pkg/log"
	"sort"
	"strings"

	ms "github.com/mesos/mesos-go/api/v1/lib"
)

// define names
const (
	ResNameMem   = "mem"
	ResNameCPUs  = "cpus"
	ResNamePorts = "ports"
)

// roles
const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

// errors
var (
	ErrNotEnoughHost     = errors.New("host is not enough with")
	ErrNotEnoughResource = errors.New("resource is not fully satisfied by the offers")

	ErrBadMasterNum = errors.New("master number must be even")
	ErrNot3Not4     = errors.New("can not deploy 4 instance with 3 host")
	ErrBadDist      = errors.New("distribution is not allow for some host have more than half nodes")
)

func getHosts(offers ...ms.Offer) []string {
	hmap := make(map[string]struct{})
	for _, offer := range offers {
		hmap[ValidateIPAddress(offer.GetHostname())] = struct{}{}
	}
	hosts := make([]string, 0)
	for h := range hmap {
		hosts = append(hosts, h)
	}
	return hosts
}

// mem will with MB
func getOfferScalar(offer ms.Offer, name string) (float64, bool) {
	for _, res := range offer.GetResources() {
		if res.GetName() == name {
			return res.GetScalar().GetValue(), true
		}
	}
	return 0.0, false
}

func getOfferRange(offer ms.Offer, name string) []int {
	for _, res := range offer.GetResources() {
		if res.GetName() == name {
			portRange := []int{}
			for _, rg := range res.GetRanges().GetRange() {
				for i := rg.GetBegin(); i <= rg.GetEnd(); i++ {
					portRange = append(portRange, int(i))
				}
			}
			return portRange
		}
	}
	return nil
}

type byCountDesc []*hostRes

func (hr byCountDesc) Len() int {
	return len(hr)
}

func (hr byCountDesc) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

func (hr byCountDesc) Less(i, j int) bool {
	return hr[i].count < hr[j].count
}

type byCountAsc []*hostRes

func (hr byCountAsc) Len() int {
	return len(hr)
}

func (hr byCountAsc) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

func (hr byCountAsc) Less(i, j int) bool {
	return hr[i].count > hr[j].count
}

func (hr byCountAsc) String() {
	for _, h := range hr {
		fmt.Printf("%v ", h)
	}
	fmt.Println()
}

type hostRes struct {
	name  string
	count int
}

func (h *hostRes) String() string {
	return fmt.Sprintf("%s %d", h.name, h.count)
}

func maxHost(hrs []*hostRes) (string, int) {
	var name = hrs[0].name
	var count = hrs[0].count
	for _, hr := range hrs {
		if count < hr.count {
			name = hr.name
			count = hr.count
		}
	}
	return name, count
}

func minInt(vals ...int) int {
	var val = vals[0]
	for _, v := range vals[1:] {
		if val > v {
			val = v
		}
	}

	return val
}

func mapIntoHostRes(offers []ms.Offer, mem float64, cpu float64) (hosts []*hostRes) {
	hosts = make([]*hostRes, 0)
	for _, offer := range offers {
		m, _ := getOfferScalar(offer, ResNameMem)
		memNode := int((m * 100.0) / (mem * 100.0))

		c, _ := getOfferScalar(offer, ResNameCPUs)
		cpuNode := int((c * 100.0) / (cpu * 100.0))

		ports := getOfferRange(offer, ResNamePorts)
		portsNode := len(ports)
		// we only need even count node
		count := minInt(memNode, cpuNode, portsNode) / 2 * 2

		log.Infof("[chunk] get the resources memory:%f cpu:%f ports-count:%d and finally count %d", m, c, len(ports), count)
		hosts = append(hosts, &hostRes{name: ValidateIPAddress(offer.GetHostname()), count: count})
	}
	return
}

func mapIntoPortsMap(offers []ms.Offer) map[string][]int {
	innerMap := make(map[string][]int)
	for _, offer := range offers {
		ports := getOfferRange(offer, ResNamePorts)
		innerMap[ValidateIPAddress(offer.GetHostname())] = ports
	}
	return innerMap
}

// dp means dynamic dispatch
// if disableHost not nil ,cannot dispatch resouce on disable host.
func dpFillHostRes(chunks []*Chunk, disableHost map[string]struct{}, hrs []*hostRes, count int, scale int) (hosts []*hostRes) {
	left := count
	hosts = make([]*hostRes, 0)
	hrmap := make(map[string]int)
	for i, hr := range hrs {
		hrmap[hr.name] = i
	}
	for _, hr := range hrs {
		hosts = append(hosts, &hostRes{name: hr.name, count: 0})
	}
	// recover chunk distribution into hosts if host exist before.
	for _, chunk := range chunks {
		idx, ok := hrmap[chunk.Nodes[0].Name]
		if ok {
			hosts[idx].count += 2
		}
		idx, ok = hrmap[chunk.Nodes[3].Name]
		if ok {
			hosts[idx].count += 2
		}
	}
	var all = len(chunks)*2 + count
	for {
		i := findMinHrs(hrs, hosts, disableHost, all, scale)
		if left == 0 {
			return
		}
		hosts[i].count += scale
		left -= scale

	}
}

func findMinHrs(hrs, hosts []*hostRes, disableHost map[string]struct{}, max, scale int) (i int) {
	var min = max
	for idx, hr := range hosts {
		if disableHost != nil {
			if _, ok := disableHost[hr.name]; ok {
				continue
			}
		}
		if hr.count < min && hrs[idx].count-hr.count >= scale {
			min = hr.count
			i = idx
		}
	}
	return
}

func findMinLink(lt [][]int, m int) int {
	row := lt[m]
	var cursor = 0
	if m == 0 {
		cursor = 1
	}

	for pos := range row {
		if pos == m {
			continue
		}
		if row[pos] < row[cursor] {
			cursor = pos
		}
	}
	return cursor
}

// link is the link between each half chunk and
// I am not Zelda !
type link struct {
	Base   string
	LinkTo string
}

// Chunk is the chunk unit for 2 master, 2 slave
type Chunk struct {
	Nodes []*Node
}

func (c *Chunk) String() string {
	var sb strings.Builder
	_, _ = sb.WriteString("Chunk<")
	nodes := make([]string, len(c.Nodes))
	for i, node := range c.Nodes {
		nodes[i] = node.String()
	}
	_, _ = sb.WriteString(strings.Join(nodes, ", "))
	_, _ = sb.WriteString(">")
	return sb.String()
}

func links2Chunks(links []link, portsMap map[string][]int) []*Chunk {
	chunks := make([]*Chunk, len(links))
	portsCountMap := make(map[string]int)
	for name := range portsMap {
		portsCountMap[name] = 0
	}

	for i, link := range links {
		var (
			baseCount   = portsCountMap[link.Base]
			linkToCount = portsCountMap[link.LinkTo]
		)

		nodes := []*Node{
			{Name: link.Base, Port: portsMap[link.Base][baseCount], Role: RoleMaster, SlaveOf: "-"},
			{Name: link.Base, Port: portsMap[link.Base][baseCount+1], Role: RoleSlave},
			{Name: link.LinkTo, Port: portsMap[link.LinkTo][linkToCount], Role: RoleMaster, SlaveOf: "-"},
			{Name: link.LinkTo, Port: portsMap[link.LinkTo][linkToCount+1], Role: RoleSlave},
		}
		portsCountMap[link.Base] = baseCount + 2
		portsCountMap[link.LinkTo] = linkToCount + 2

		chunks[i] = &Chunk{nodes}
	}
	return chunks
}

func checkIfEnough(hrs []*hostRes, need int) bool {
	sum := 0
	for _, hr := range hrs {
		sum += hr.count
	}

	return sum >= need
}

func checkDist(hrs []*hostRes, count int) bool {
	for _, hr := range hrs {
		if hr.count >= count/2 {
			return false
		}
	}
	return true
}

// Chunks will chunks the given offer.
func Chunks(masterNum int, memory, cpu float64, offers ...ms.Offer) (chunks []*Chunk, err error) {
	hrs, err := checkChunk(nil, masterNum, memory, cpu, offers...)
	if err != nil {
		return
	}
	if !checkDist(hrs, masterNum*2) {
		err = ErrBadDist
		return
	}

	hrmap := make(map[string]int)
	for i, hr := range hrs {
		hrmap[hr.name] = i
	}
	hcount := len(hrs)

	linkTable := make([][]int, hcount)
	for i := 0; i < hcount; i++ {
		linkTable[i] = make([]int, hcount)
		for j := 0; j < hcount; j++ {
			linkTable[i][j] = 0
		}
	}

	links := []link{}
	for {
		name, count := maxHost(hrs)
		if count == 0 {
			break
		}
		m := hrmap[name]
		llh := findMinLink(linkTable, m)
		if hrs[llh].count < 2 {
			linkTable[llh][m]++
			linkTable[m][llh]++
			continue
		}
		llHost := hrs[llh]
		links = append(links, link{Base: name, LinkTo: llHost.name})
		linkTable[llh][m]++
		linkTable[m][llh]++
		hrs[m].count -= 2
		hrs[llh].count -= 2
	}
	portsMap := mapIntoPortsMap(offers)
	chunks = links2Chunks(links, portsMap)
	return
}

// GetHostCountInChunks will calc hosts table
func GetHostCountInChunks(chunks []*Chunk) map[string][]int {
	hostmap := make(map[string][]int)
	for _, chunk := range chunks {
		for _, node := range chunk.Nodes {
			if ports, ok := hostmap[node.Name]; ok {
				ports = append(ports, node.Port)
				hostmap[node.Name] = ports
			} else {
				hostmap[node.Name] = []int{node.Port}
			}
		}
	}
	return hostmap
}

// ChunksAppend scale masternum with origin chunks.
func ChunksAppend(chunks []*Chunk, masterNum int, memory, cpu float64, offers ...ms.Offer) (newChunks []*Chunk, err error) {
	hrs, err := checkChunk(chunks, masterNum, memory, cpu, offers...)
	if err != nil {
		return
	}
	if !checkDist(hrs, (len(chunks)*2+masterNum)*2) {
		err = ErrBadDist
		return
	}
	hrmap := make(map[string]int)
	for i, hr := range hrs {
		hrmap[hr.name] = i
	}
	hcount := len(hrs)

	linkTable := make([][]int, hcount)
	for i := 0; i < hcount; i++ {
		linkTable[i] = make([]int, hcount)
		for j := 0; j < hcount; j++ {
			linkTable[i][j] = 0
		}
	}
	links := []link{}
	// recover chunks to linktable
	for _, chunk := range chunks {
		base := chunk.Nodes[0].Name
		linkto := chunk.Nodes[2].Name
		i, oki := hrmap[base]
		if oki {
			hrs[i].count -= 2
		}
		j, okj := hrmap[linkto]
		if okj {
			hrs[j].count -= 2
		}
		if oki && okj {
			linkTable[i][j]++
			linkTable[j][i]++
		}
	}

	for {
		name, count := maxHost(hrs)
		if count == 0 {
			break
		}
		m := hrmap[name]
		llh := findMinLink(linkTable, m)
		// with more offer host,may distribution before.
		if hrs[llh].count < 2 {
			linkTable[llh][m]++
			linkTable[m][llh]++
			continue
		}
		llHost := hrs[llh]
		links = append(links, link{Base: name, LinkTo: llHost.name})
		hrs[m].count -= 2
		hrs[llh].count -= 2
	}
	portsMap := mapIntoPortsMap(offers)
	newChunks = links2Chunks(links, portsMap)
	return
}

func checkChunk(chunk []*Chunk, masterNum int, memory, cpu float64, offers ...ms.Offer) (hrs []*hostRes, err error) {
	if masterNum%2 != 0 {
		err = ErrBadMasterNum
		return
	}
	hosts := getHosts(offers...)
	if len(hosts) < 3 {
		err = ErrNotEnoughHost
		return
	}
	if len(hosts) == 3 && masterNum == 4 {
		err = ErrNot3Not4
		return
	}
	hrs = mapIntoHostRes(offers, memory, cpu)

	if !checkIfEnough(hrs, masterNum*2) {
		err = ErrNotEnoughResource
		return
	}
	sort.Sort(byCountDesc(hrs))
	hrmap := make(map[string]int)
	for i, hr := range hrs {
		hrmap[hr.name] = i
	}
	hrs = dpFillHostRes(chunk, nil, hrs, masterNum*2, 2) // NOTICE: each master is a half chunk
	return
}

// ChunksRecover recover chunk with disabled host by new offers.
func ChunksRecover(chunks []*Chunk, host string, memory, cpu float64, offers ...ms.Offer) (newChunk []*Chunk, err error) {
	var (
		relateHost = make(map[string]struct{}, 0)
		dpCount    int
		allCount   = len(chunks) * 4
	)
	newChunk = make([]*Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.Nodes[0].Name == host {
			relateHost[chunk.Nodes[2].Name] = struct{}{}
			dpCount += 2
			continue
		}
		if chunk.Nodes[3].Name == host {
			relateHost[chunk.Nodes[0].Name] = struct{}{}
			dpCount += 2
			continue
		}
		newChunk = append(newChunk, chunk)
	}
	hrs := mapIntoHostRes(offers, memory, cpu)
	if !checkIfEnough(hrs, dpCount) {
		err = ErrNotEnoughResource
		return
	}
	sort.Sort(byCountDesc(hrs))
	hrmap := make(map[string]int)
	for i, hr := range hrs {
		hrmap[hr.name] = i
	}
	hrs = dpFillHostRes(chunks, relateHost, hrs, dpCount, 2)
	if !checkDist(hrs, allCount) {
		err = ErrBadDist
		return
	}
	hcount := len(hrs)

	linkTable := make([][]int, hcount)
	for i := 0; i < hcount; i++ {
		linkTable[i] = make([]int, hcount)
		for j := 0; j < hcount; j++ {
			linkTable[i][j] = 0
		}
	}
	links := []link{}
	// recover chunks to linktable
	for _, chunk := range chunks {
		base := chunk.Nodes[0].Name
		linkto := chunk.Nodes[2].Name
		i, oki := hrmap[base]
		if oki {
			hrs[i].count -= 2
		}
		j, okj := hrmap[linkto]
		if okj {
			hrs[j].count -= 2
		}
		if oki && okj {
			linkTable[i][j]++
			linkTable[j][i]++
		}
	}
	for name := range relateHost {
		m := hrmap[name]
		var llh int
		for {
			llh = findMinLink(linkTable, m)
			if hrs[llh].count < 2 {
				linkTable[llh][m]++
				linkTable[m][llh]++
				continue
			}
			break
		}
		llHost := hrs[llh]
		links = append(links, link{Base: name, LinkTo: llHost.name})
		hrs[llh].count -= 2
	}
	portsMap := mapIntoPortsMap(offers)
	tmpChunk := links2Chunks(links, portsMap)
	newChunk = append(newChunk, tmpChunk...)
	return
}
