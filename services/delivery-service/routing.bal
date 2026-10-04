// =====================================================================
//  Route optimisation (Bonus)
//  A weighted road graph of Windhoek suburbs. Edge weight = travel time in
//  minutes (distance * road factor / road speed). Dijkstra's algorithm finds
//  the fastest path between the graph nodes nearest to the start and end
//  coordinates; the first and last mile use local street speed.
// =====================================================================

type GraphNode record {|
    string name;
    float lat;
    float lng;
|};

type RoadEdge record {|
    string a;
    string b;
    float speedKmh;
|};

type RouteResult record {|
    float[][] path; // [[lat, lng], ...]
    string[] via; // suburbs traversed
    float distanceKm;
    int etaMinutes;
|};

const float ROAD_FACTOR = 1.25; // straight line -> road distance
const float LOCAL_SPEED_KMH = 30.0;

final readonly & GraphNode[] NODES = [
    {name: "CBD", lat: -22.5609, lng: 17.0836},
    {name: "Windhoek West", lat: -22.5655, lng: 17.0700},
    {name: "Klein Windhoek", lat: -22.5710, lng: 17.1010},
    {name: "Eros", lat: -22.5475, lng: 17.0950},
    {name: "Ludwigsdorf", lat: -22.5790, lng: 17.1120},
    {name: "Olympia", lat: -22.5895, lng: 17.0860},
    {name: "Pionierspark", lat: -22.5960, lng: 17.0650},
    {name: "Academia", lat: -22.6005, lng: 17.0950},
    {name: "Kleine Kuppe", lat: -22.6190, lng: 17.0910},
    {name: "Auasblick", lat: -22.6080, lng: 17.1060},
    {name: "Khomasdal", lat: -22.5540, lng: 17.0500},
    {name: "Katutura", lat: -22.5250, lng: 17.0600},
    {name: "Wanaheda", lat: -22.5150, lng: 17.0450},
    {name: "Hochland Park", lat: -22.5660, lng: 17.0450},
    {name: "Northern Industrial", lat: -22.5340, lng: 17.0760},
    {name: "Dorado Park", lat: -22.5450, lng: 17.0400},
    {name: "Otjomuise", lat: -22.5480, lng: 17.0230}
];

final readonly & RoadEdge[] EDGES = [
    {a: "CBD", b: "Windhoek West", speedKmh: 45.0},
    {a: "CBD", b: "Klein Windhoek", speedKmh: 50.0},
    {a: "CBD", b: "Eros", speedKmh: 50.0},
    {a: "CBD", b: "Northern Industrial", speedKmh: 55.0},
    {a: "CBD", b: "Olympia", speedKmh: 60.0},
    {a: "Windhoek West", b: "Khomasdal", speedKmh: 50.0},
    {a: "Windhoek West", b: "Hochland Park", speedKmh: 45.0},
    {a: "Windhoek West", b: "Pionierspark", speedKmh: 50.0},
    {a: "Klein Windhoek", b: "Ludwigsdorf", speedKmh: 45.0},
    {a: "Klein Windhoek", b: "Eros", speedKmh: 45.0},
    {a: "Klein Windhoek", b: "Olympia", speedKmh: 55.0},
    {a: "Ludwigsdorf", b: "Auasblick", speedKmh: 50.0},
    {a: "Olympia", b: "Academia", speedKmh: 50.0},
    {a: "Olympia", b: "Pionierspark", speedKmh: 45.0},
    {a: "Academia", b: "Kleine Kuppe", speedKmh: 55.0},
    {a: "Academia", b: "Auasblick", speedKmh: 45.0},
    {a: "Auasblick", b: "Kleine Kuppe", speedKmh: 45.0},
    {a: "Pionierspark", b: "Kleine Kuppe", speedKmh: 70.0}, // Western Bypass
    {a: "Khomasdal", b: "Katutura", speedKmh: 50.0},
    {a: "Khomasdal", b: "Hochland Park", speedKmh: 45.0},
    {a: "Khomasdal", b: "Dorado Park", speedKmh: 45.0},
    {a: "Katutura", b: "Wanaheda", speedKmh: 40.0},
    {a: "Katutura", b: "Northern Industrial", speedKmh: 50.0},
    {a: "Wanaheda", b: "Otjomuise", speedKmh: 70.0}, // Western Bypass
    {a: "Dorado Park", b: "Otjomuise", speedKmh: 45.0},
    {a: "Hochland Park", b: "Otjomuise", speedKmh: 45.0},
    {a: "Otjomuise", b: "Pionierspark", speedKmh: 70.0} // Western Bypass
];

isolated function nodeIndex(string name) returns int {
    foreach int i in 0 ..< NODES.length() {
        if NODES[i].name == name {
            return i;
        }
    }
    return -1;
}

isolated function nearestNode(float lat, float lng) returns int {
    int best = 0;
    float bestKm = float:Infinity;
    foreach int i in 0 ..< NODES.length() {
        float d = haversineKm(lat, lng, NODES[i].lat, NODES[i].lng);
        if d < bestKm {
            bestKm = d;
            best = i;
        }
    }
    return best;
}

isolated function edgeKm(int i, int j) returns float =>
    haversineKm(NODES[i].lat, NODES[i].lng, NODES[j].lat, NODES[j].lng) * ROAD_FACTOR;

// Dijkstra over travel time. Returns node indices from src to dst (inclusive).
isolated function dijkstra(int src, int dst) returns int[] {
    int n = NODES.length();
    // adjacency matrix of minutes (Infinity = no road)
    float[][] w = [];
    foreach int i in 0 ..< n {
        float[] row = [];
        foreach int j in 0 ..< n {
            row.push(i == j ? 0.0 : float:Infinity);
        }
        w.push(row);
    }
    foreach RoadEdge e in EDGES {
        int i = nodeIndex(e.a);
        int j = nodeIndex(e.b);
        if i >= 0 && j >= 0 {
            float minutes = edgeKm(i, j) / e.speedKmh * 60.0;
            w[i][j] = minutes;
            w[j][i] = minutes;
        }
    }
    float[] dist = [];
    int[] prev = [];
    boolean[] done = [];
    foreach int i in 0 ..< n {
        dist.push(float:Infinity);
        prev.push(-1);
        done.push(false);
    }
    dist[src] = 0.0;
    foreach int _ in 0 ..< n {
        int u = -1;
        foreach int i in 0 ..< n {
            if !done[i] && (u == -1 || dist[i] < dist[u]) {
                u = i;
            }
        }
        if u == -1 || dist[u] == float:Infinity || u == dst {
            break;
        }
        done[u] = true;
        foreach int v in 0 ..< n {
            if !done[v] && w[u][v] != float:Infinity && dist[u] + w[u][v] < dist[v] {
                dist[v] = dist[u] + w[u][v];
                prev[v] = u;
            }
        }
    }
    int[] path = [];
    int cur = dst;
    while cur != -1 {
        path.unshift(cur);
        if cur == src {
            break;
        }
        cur = prev[cur];
    }
    if path.length() == 0 || path[0] != src {
        return [src, dst]; // disconnected graph fallback
    }
    return path;
}

// Fastest route between two coordinates.
isolated function shortestRoute(float fromLat, float fromLng, float toLat, float toLng) returns RouteResult {
    int s = nearestNode(fromLat, fromLng);
    int t = nearestNode(toLat, toLng);
    float directKm = haversineKm(fromLat, fromLng, toLat, toLng) * ROAD_FACTOR;
    // Very short trips: drive directly on local streets.
    if s == t || directKm < 1.0 {
        return {
            path: [[fromLat, fromLng], [toLat, toLng]],
            via: [NODES[s].name],
            distanceKm: roundKm(directKm),
            etaMinutes: minutesCeil(directKm / LOCAL_SPEED_KMH * 60.0)
        };
    }
    int[] nodes = dijkstra(s, t);
    float[][] path = [[fromLat, fromLng]];
    string[] via = [];
    float km = haversineKm(fromLat, fromLng, NODES[s].lat, NODES[s].lng) * ROAD_FACTOR;
    float minutes = km / LOCAL_SPEED_KMH * 60.0;
    foreach int k in 0 ..< nodes.length() {
        GraphNode node = NODES[nodes[k]];
        path.push([node.lat, node.lng]);
        via.push(node.name);
        if k > 0 {
            float segKm = edgeKm(nodes[k - 1], nodes[k]);
            km += segKm;
            minutes += segKm / speedBetween(nodes[k - 1], nodes[k]) * 60.0;
        }
    }
    float lastKm = haversineKm(NODES[t].lat, NODES[t].lng, toLat, toLng) * ROAD_FACTOR;
    km += lastKm;
    minutes += lastKm / LOCAL_SPEED_KMH * 60.0;
    path.push([toLat, toLng]);
    return {path, via, distanceKm: roundKm(km), etaMinutes: minutesCeil(minutes)};
}

// Multi-stop route: driver -> restaurant -> customer
isolated function combineRoutes(RouteResult first, RouteResult second) returns RouteResult {
    float[][] path = [...first.path];
    foreach int i in 1 ..< second.path.length() {
        path.push(second.path[i]);
    }
    string[] via = [...first.via];
    foreach string v in second.via {
        if via.length() == 0 || via[via.length() - 1] != v {
            via.push(v);
        }
    }
    return {
        path,
        via,
        distanceKm: roundKm(first.distanceKm + second.distanceKm),
        etaMinutes: first.etaMinutes + second.etaMinutes
    };
}

isolated function speedBetween(int i, int j) returns float {
    foreach RoadEdge e in EDGES {
        int a = nodeIndex(e.a);
        int b = nodeIndex(e.b);
        if (a == i && b == j) || (a == j && b == i) {
            return e.speedKmh;
        }
    }
    return LOCAL_SPEED_KMH;
}

isolated function roundKm(float km) returns float => float:round(km * 100.0) / 100.0;

isolated function minutesCeil(float minutes) returns int {
    int m = <int>float:ceiling(minutes);
    return m < 1 ? 1 : m;
}
