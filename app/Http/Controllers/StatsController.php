<?php

namespace App\Http\Controllers;

use App\Stat;

class StatsController extends Controller
{
    /**
     * Get stats
     *
     * @return \Illuminate\Http\Response
     */
    public function index()
    {
        $stats = Stat::selectRaw('count(make) as total, stats.*')
            ->groupBy('make')
            ->groupBy('model')
            ->orderBy('total', 'desc')
            ->get();

        return view('stats.index', ['stats' => $stats]);
    }
}
